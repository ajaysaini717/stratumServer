package server

/*
#cgo CFLAGS: -I${SRCDIR}
#include "scrypt.h"
*/
import "C"

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"asicPool/internal/logger"
	"asicPool/internal/pool/pplns"
	"asicPool/internal/pool/storage"
	"asicPool/internal/pool/wallet"
)

// "os"

// Config controls how the stratum server connects to upstream RPC endpoints and
// exposes the listener for miners.
type Config struct {
	ListenAddr      string
	RPCURL          string
	WalletRPCURL    string // EVM RPC for Payouts
	RPCUser         string
	RPCPassword     string
	PPLNSTargetWork float64
	// Maturity Configuration
	BlockMaturityDepth  int // Confirmations for Balance Maturity (Default 10)
	PayoutMaturityDepth int // Confirmations for Payout Unlock (Default 4096)
	PoolFeePercentage   float64
	PGURL               string
	PoolPrivateKey      string // Hex private key for payouts
	IPFSNodeURL         string
	LogRotationInterval string // e.g., "1h", "10m"
}

// RPC config
var (
	listenAddr  = ":3334"
	rpcURL      = "http://127.0.0.1:38131"
	rpcUser     = "test"
	rpcPassword = "test"

	// PPLNS
	PPLNSWindow *pplns.Window
)

// Static subscribe reply
var (
	subscribeExtranonce2Size = 4
	acceptedJobs             sync.Map // jobID(string) -> *JobData
	en1Counter               uint32   // for generating unique extranonce1
	templateSeqCounter       atomic.Uint64
)

type JobData struct {
	Version         uint32
	TxRoot          []byte
	StateRoot       []byte
	NBits           uint32
	Height          []byte // Block height (LE)
	CoinbaseAddress []byte
	Reward          []byte    // LittleEndian uint64
	BlockFees       []byte    // LittleEndian uint64
	ParentHash      string    // normalized (no 0x, lowercase) previous hash
	NTime           uint32    // template curtime
	CreatedAt       time.Time // when we created/stored this job
	TemplateSeq     uint64
	// Difficulty snapshot for this job (VarDiff pdiff)
	Difficulty float64
	// Previous difficulty snapshot (for JS-style fallback)
	PrevDiff float64

	stale          atomic.Bool
	blockSubmitted atomic.Bool
	usedNonces     sync.Map // Prevent duplicate shares: key="nonce-en2-ntime" -> bool
}

// ------------------- Stratum Structures -------------------

type stratumRequest struct {
	ID     interface{}       `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

type stratumResponse struct {
	ID     interface{} `json:"id,omitempty"`
	Result interface{} `json:"result,omitempty"`
	Error  interface{} `json:"error,omitempty"`
}

type client struct {
	conn   net.Conn
	rw     *bufio.ReadWriter
	mu     sync.Mutex
	closed bool
	addr   string

	diffMu               sync.Mutex
	pdiff                float64 // current VarDiff difficulty
	prevPdiff            float64 // previous difficulty
	windowStart          time.Time
	windowShares         int
	lastAdjust           time.Time
	username             string
	authorized           bool
	extranonce1          [4]byte //unique per connection
	sessionID            string  // Unique session ID for Stratum
	extranonceSubscribed bool

	// Config Reference
	cfg        *Config
	storage    storage.Storage
	ipfsLogger *logger.IPFSLogger
}

type BlockTemplate struct {
	Version int `json:"version"`
	Parents []struct {
		Data string `json:"data"`
	} `json:"parents"`
	TxRoot           string `json:"txroot"`
	StateRoot        string `json:"stateroot"`
	PoWDiffReference struct {
		Nbits string `json:"nbits"`
	} `json:"pow_diff_reference"`
	CurTime         int    `json:"curtime"`
	PreviousHash    string `json:"previousblockhash"`
	Height          int64  `json:"height"`
	CoinbaseAddress string `json:"coinbase_address"`
	Reward          int64  `json:"reward"`
	BlockFees       int64  `json:"block_fees"`
}

var clientsMu sync.Mutex
var clients = map[*client]struct{}{}

// Run boots the stratum server and blocks while serving miner connections.
func Run(cfg Config) error {
	applyConfig(cfg)
	if cfg.PoolPrivateKey != "" {
		pkHex := cfg.PoolPrivateKey
		if strings.HasPrefix(pkHex, "0x") {
			pkHex = pkHex[2:]
		}
		privateKey, err := crypto.HexToECDSA(pkHex)
		if err != nil {
			log.Printf("⚠️ Failed to parse PoolPrivateKey: %v", err)
		} else {
			publicKey := privateKey.Public()
			publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
			if !ok {
				log.Printf("⚠️ Failed to cast public key to ECDSA")
			} else {
				addr := crypto.PubkeyToAddress(*publicKeyECDSA)
				poolAddress = addr.Hex()
				log.Printf("✅ Pool Operator Address Derived: %s", poolAddress)
			}
		}
	}
	// Initialize PPLNS Window
	targetWork := cfg.PPLNSTargetWork
	if targetWork <= 0 {
		targetWork = 1000.0 // Default fallback if not set
		log.Printf("⚠️ PPLNS target work not set, defaulting to %.1f", targetWork)
	}
	PPLNSWindow = pplns.NewWindow(targetWork)
	log.Printf("PPLNS initialized with target work: %.1f", targetWork)

	// Maturity Defaults
	if cfg.BlockMaturityDepth <= 0 {
		cfg.BlockMaturityDepth = 10
	}
	if cfg.PayoutMaturityDepth <= 0 {
		cfg.PayoutMaturityDepth = 4096
	}
	log.Printf("Maturity Policy: Balance=%d blocks, Payout=%d blocks", cfg.BlockMaturityDepth, cfg.PayoutMaturityDepth)

	// Initialize Storage (if configured)
	var poolStorage storage.Storage
	if cfg.PGURL != "" {
		pg := storage.NewPostgresStorage(cfg.PGURL)
		if err := pg.Connect(context.Background()); err != nil {
			log.Printf("❌ Database Connection Failed: %v", err)
			return err
		}
		defer pg.Close()
		poolStorage = pg
	} else {
		log.Println("⚠️ Running WITHOUT Database Persistence (PG_URL not set)")
	}

	// Initialize IPFS Logger
	var ipfsLog *logger.IPFSLogger
	if cfg.IPFSNodeURL != "" {
		ipfsLog = logger.NewIPFSLogger(cfg.IPFSNodeURL)
		log.Printf("✅ IPFS Logger Initialized (Node: %s)", cfg.IPFSNodeURL)
	}

	// Initialize Wallet Client
	var walletClient wallet.Client
	if cfg.WalletRPCURL != "" && cfg.PoolPrivateKey != "" {
		// Inject Basic Auth credentials into URL if present
		authURL := cfg.WalletRPCURL
		// Only inject auth if user provided it AND it's not already in the URL
		if cfg.RPCUser != "" && cfg.RPCPassword != "" && !strings.Contains(authURL, "@") {
			parts := strings.Split(authURL, "://")
			if len(parts) == 2 {
				// We assume RPCUser/Pass applies to both interfaces if set,
				// or user can embed credentials in WALLET_RPC_URL directly.
				authURL = fmt.Sprintf("%s://%s:%s@%s", parts[0], cfg.RPCUser, cfg.RPCPassword, parts[1])
			}
		}

		w, err := wallet.NewEthWalletClient(context.Background(), authURL, cfg.PoolPrivateKey)
		if err != nil {
			log.Printf("⚠️ Failed to connect to Wallet RPC: %v (Payouts will stay PENDING)", err)
		} else {
			walletClient = w
			defer walletClient.Close()
			log.Println("✅ Wallet Client Connected (Local Signing)")
		}
	} else {
		log.Println("⚠️ Wallet RPC or Private Key missing. Payouts will NOT be processed.")
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	// Graceful Shutdown Channel
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	shutdownChan := make(chan struct{})

	// WaitGroup for client connections
	var wg sync.WaitGroup

	go func() {
		<-sigChan
		close(shutdownChan)
		log.Println("⚠️  Shutdown signal received. Closing listener...")
		ln.Close() // Stop accepting NEW connections

		// Optional: Close existing connections immediately or wait?
		// For a pool, we usually cut connections so they failover to backup pools.
		clientsMu.Lock()
		count := len(clients)
		log.Printf("Check: %d active clients to disconnect", count)
		for c := range clients {
			c.conn.Close() // Force close to unblock Read/Write
		}
		clientsMu.Unlock()
	}()

	log.Printf("Stratum server is listening on %s", listenAddr)

	// Start background services
	go parentWatcher()
	go jobBroadcaster()
	go jobReaper()
	go blockLifecycleManager(poolStorage, walletClient, cfg.BlockMaturityDepth, cfg.PayoutMaturityDepth)

	// Parse Log Rotation Interval
	rotationInterval := 1 * time.Hour
	if cfg.LogRotationInterval != "" {
		if d, err := time.ParseDuration(cfg.LogRotationInterval); err == nil {
			rotationInterval = d
		} else {
			log.Printf("⚠️ Invalid LOG_ROTATION_INTERVAL '%s', defaulting to 1h: %v", cfg.LogRotationInterval, err)
		}
	}
	go StartLogRotationManager(poolStorage, ipfsLog, rotationInterval)

	// broadcaster needs no changes, it just runs.

	for {
		conn, err := ln.Accept()
		if err != nil {
			// If shutdown closed the listener, we exit loop
			select {
			case <-shutdownChan:
				return nil
			default:
			}
			log.Printf("accept err: %v", err)
			continue
		}
		c := &client{
			conn:       conn,
			rw:         bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
			addr:       conn.RemoteAddr().String(),
			cfg:        &cfg, // Inject Config
			storage:    poolStorage,
			ipfsLogger: ipfsLog,
		}

		clientsMu.Lock()
		clients[c] = struct{}{}
		clientsMu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			serveClient(c)
		}()
	}
}

func applyConfig(cfg Config) {
	if cfg.ListenAddr != "" {
		listenAddr = cfg.ListenAddr
	}
	if cfg.RPCURL != "" {
		rpcURL = cfg.RPCURL
	}
	// wallet RPC not used in global vars
	if cfg.RPCUser != "" {
		rpcUser = cfg.RPCUser
	}
	if cfg.RPCPassword != "" {
		rpcPassword = cfg.RPCPassword
	}
	if cfg.RPCPassword != "" {
		rpcPassword = cfg.RPCPassword
	}
}

func serveClient(c *client) {
	defer func() {
		c.conn.Close()
		clientsMu.Lock()
		delete(clients, c)
		clientsMu.Unlock()
	}()

	// Standard Stratum: Roll extranonce every 20 minutes to prevent nonce exhaustion
	stopRolling := make(chan struct{})
	defer close(stopRolling)
	go func() {
		ticker := time.NewTicker(20 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-stopRolling:
				return
			case <-ticker.C:
				// Try to roll (helper.go will check if subscribed)
				sendSetExtranonce(c)
			}
		}
	}()

	// DoS Protection:
	// 1. Max Message Size: 10KB (prevent memory exhaustion)
	// 2. Read Deadline: 10 minutes (disconnect idle/slowloris)
	scanner := bufio.NewScanner(c.conn)
	buf := make([]byte, 1024)
	scanner.Buffer(buf, 10*1024) // 10KB max

	for {
		// Reset deadling before every read
		_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Minute))

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					log.Printf("[%s] read error: %v", c.addr, err)
				}
			} else {
				// EOF
			}
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var req stratumRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			log.Printf("[%s] invalid json: %v -- %s", c.addr, err, line)
			continue
		}
		go handleRequest(c, &req)
	}
}

func handleRequest(c *client, req *stratumRequest) {
	switch req.Method {
	case "mining.subscribe":
		handleSubscribe(c, req)
	case "mining.authorize":
		handleAuthorize(c, req)
	case "mining.extranonce.subscribe":
		handleExtranonceSubscribe(c, req)
	case "mining.submit":
		if err := handleSubmit(c, req); err != nil {
			log.Printf("[ERROR]Error handling submit for %s: %v", c.addr, err)
			c.mu.Lock()
			c.closed = true
			c.mu.Unlock()
			c.conn.Close()
		}
	default:
		sendError(c, req.ID, -32601, "Method not found")
	}
}

// ------------------- Crypto / Hashing -------------------

func scryptHash(header []byte) common.Hash {
	out := make([]byte, 32)

	C.scrypt_hash_data(
		unsafe.Pointer(&out[0]),
		unsafe.Pointer(&header[0]),
	)
	return common.BytesToHash(out)
}
