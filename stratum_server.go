package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/hex"
	"math/big"

	blake2sext "github.com/ajaysaini717/myalgo"
	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Payout struct {
	JobID  uint64
	Addr   string
	Amount float64
}

type StratumMessage struct {
	ID     interface{}   `json:"id"`
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}
type StratumResponse struct {
	ID     interface{} `json:"id"`
	Result interface{} `json:"result"`
	Error  interface{} `json:"error"`
}

var (
	magicNum    = "2211"
	rpcURL      = "http://127.0.0.1:38131"
	evmURL      = "http://127.0.0.1:18545"
	rpcUser     = "test"
	rpcPassword = "test"
	privKeyHex  = "b54bd72f6cecd4811309608da20315b449b344db5faabe3b9de8e9f3bbde5267"

	clients   = make(map[*Client]struct{})
	clientsMu sync.RWMutex

	shareFactor = 2
	jobMu       sync.RWMutex
	currentJob  *Job
	lastJob     *Job
	lastJobTs   time.Time
	poolFeePct  = 5 // 5% pool fee

	ethClient  *ethclient.Client
	senderKey  *ecdsa.PrivateKey
	senderAddr common.Address

	nextNonce   uint64
	nonceMu     sync.Mutex
	payoutQueue chan Payout
)

func init() {
	var err error

	// 1) Dial RPC once
	ethClient, err = ethclient.Dial(evmURL)
	if err != nil {
		log.Fatalf("failed to connect to EVM RPC: %v", err)
	}

	// 2) Parse private key once
	pkBytes, err := hex.DecodeString(privKeyHex)
	if err != nil {
		log.Fatalf("invalid privKeyHex: %v", err)
	}
	senderKey, err = crypto.ToECDSA(pkBytes)
	if err != nil {
		log.Fatalf("invalid ECDSA key: %v", err)
	}
	senderAddr = crypto.PubkeyToAddress(senderKey.PublicKey)
	log.Printf("Payout sender address: %s", senderAddr.Hex())

	// 3) Initialize nonce from pending
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := ethClient.PendingNonceAt(ctx, senderAddr)
	if err != nil {
		log.Fatalf("failed to fetch pending nonce: %v", err)
	}
	nonceMu.Lock()
	nextNonce = n
	nonceMu.Unlock()

	// 4) Start single payout worker
	payoutQueue = make(chan Payout, 4000)
	go func() {
		for task := range payoutQueue {
			txid, err := sendToAddressRPC(task.Addr, task.Amount)
			if err != nil {
				log.Printf("❌ payout job %d to %s failed: %v",
					task.JobID, task.Addr, err)
			} else {
				log.Printf("✅ payout job %d to %s (%.8f ETH) tx=%s",
					task.JobID, task.Addr, task.Amount, txid)
			}
		}
	}()
}

type Job struct {
	ID          uint64
	HeaderHex   string
	Target      string
	ShareTarget string
	CreatedAt   time.Time
	Reward      uint64
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
	Reward          uint64 `json:"reward"`
}

type Client struct {
	conn        net.Conn
	authorized  bool
	subscribed  bool
	mu          sync.Mutex
	shareCounts map[uint64]uint64
	currentJob  *Job
	MinerAddr   string
}

func getBlockTemplate() (*BlockTemplate, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getBlockTemplate",
		"params":  []interface{}{[]interface{}{}, 9},
	}
	buf, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", rpcURL, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(rpcUser, rpcPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var rpcResp struct {
		Result BlockTemplate `json:"result"`
		Error  interface{}   `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		fmt.Printf("ERORROR : %+v", rpcResp.Error)
		return nil, errors.New("RPC error in getBlockTemplate")
	}
	return &rpcResp.Result, nil
}

func CompactToBig(compact uint32) *big.Int {

	mantissa := compact & 0x007fffff
	isNegative := compact&0x00800000 != 0
	exponent := uint(compact >> 24)

	var bn *big.Int
	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		bn = big.NewInt(int64(mantissa))
	} else {
		bn = big.NewInt(int64(mantissa))
		bn.Lsh(bn, 8*(exponent-3))
	}

	if isNegative {
		bn = bn.Neg(bn)
	}

	return bn
}

func buildJob(tpl *BlockTemplate, id uint64) (*Job, error) {
	header := make([]byte, 144)
	off := 0
	binary.LittleEndian.PutUint32(header[off:], uint32(tpl.Version))
	off += 4

	prev, err := hex.DecodeString(strings.TrimPrefix(tpl.PreviousHash, "0x"))
	if err != nil || len(prev) != 32 {
		return nil, fmt.Errorf("invalid previous hash length")
	}
	copy(header[off:], prev)
	off += 32

	txRoot, _ := hex.DecodeString(strings.TrimPrefix(tpl.TxRoot, "0x"))
	copy(header[off:], txRoot)
	off += 32

	stateRoot, _ := hex.DecodeString(strings.TrimPrefix(tpl.StateRoot, "0x"))
	copy(header[off:], stateRoot)
	off += 32

	nbits, _ := strconv.ParseUint(tpl.PoWDiffReference.Nbits, 16, 32)
	binary.LittleEndian.PutUint32(header[off:], uint32(nbits))
	off += 4

	binary.LittleEndian.PutUint32(header[off:], uint32(tpl.CurTime))
	off += 4
	binary.LittleEndian.PutUint64(header[off:], uint64(tpl.Height))
	off += 8

	coinbase, _ := hex.DecodeString(strings.TrimPrefix(tpl.CoinbaseAddress, "0x"))
	copy(header[off:], coinbase)
	off += 20
	binary.LittleEndian.PutUint64(header[off:], tpl.Reward)
	off += 8

	header = append(header, []byte{9, 0, 0, 0, 0, 0}...)
	headerHex := hex.EncodeToString(header)

	comBits := CompactToBig(uint32(nbits)).Text(16)
	target := fmt.Sprintf("%64s", comBits)
	target = strings.ReplaceAll(target, " ", "0")

	comBitsShare := new(big.Int).Mul(CompactToBig(uint32(nbits)), big.NewInt(int64(shareFactor))).Text(16)
	sharetarget := fmt.Sprintf("%64s", comBitsShare)
	sharetarget = strings.ReplaceAll(sharetarget, " ", "0")

	return &Job{ID: id, HeaderHex: headerHex, Target: target, ShareTarget: sharetarget, CreatedAt: time.Now(), Reward: tpl.Reward}, nil
}

func handleConnection(conn net.Conn) {
	fmt.Println("YES, new client connected")
	client := &Client{
		conn:        conn,
		authorized:  false,
		subscribed:  false,
		shareCounts: make(map[uint64]uint64),
	}
	clientsMu.Lock()
	clients[client] = struct{}{}
	clientsMu.Unlock()

	defer func() {
		clientsMu.Lock()
		delete(clients, client)
		clientsMu.Unlock()
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("Error reading from client: %v", err)
			return
		}

		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}

		var msg StratumMessage
		if err := json.Unmarshal(trimmed, &msg); err != nil {
			log.Printf("Error unmarshaling JSON: %v", err)
			sendErrorResponse(conn, nil, -32700, "Parse error")
			continue
		}

		log.Printf("Received message: %+v", msg)

		switch msg.Method {
		case "mining.subscribe":
			handleSubscribe(client, msg.ID)
		case "mining.authorize":
			handleAuthorize(client, msg.ID, msg.Params)
		case "mining.submit":
			handleSubmit(client, msg.ID, msg.Params)
		default:
			sendErrorResponse(conn, msg.ID, -32601, "Method not found")
		}
	}
}

func broadcastNotify(job *Job) {
	targetMsg := StratumMessage{
		ID:     nil,
		Method: "mining.set_target",
		Params: []interface{}{job.ShareTarget},
	}
	notifyMsg := StratumMessage{
		ID:     nil,
		Method: "mining.notify",
		Params: []interface{}{
			strconv.FormatUint(job.ID, 10),
			job.HeaderHex,
			true,
		},
	}
	bSet, _ := json.Marshal(targetMsg)
	bNotify, _ := json.Marshal(notifyMsg)

	clientsMu.RLock()
	clientsList := make([]*Client, 0, len(clients))
	for c := range clients {
		clientsList = append(clientsList, c)
	}
	clientsMu.RUnlock()
	var wg sync.WaitGroup
	wg.Add(len(clientsList))

	for _, c := range clientsList {
		go func(c *Client) {
			defer wg.Done()
			c.mu.Lock()
			defer c.mu.Unlock()
			if !c.subscribed || !c.authorized {
				return
			}
			c.currentJob = job
			// send set_target
			c.conn.Write(append(bSet, '\n'))
			// send notify
			c.conn.Write(append(bNotify, '\n'))
		}(c)
	}
	wg.Wait()
}

type templateFetcher struct{}

func (t *templateFetcher) Start() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var nextID uint64
	var lastFP string
	for range ticker.C {
		tpl, err := getBlockTemplate()
		if err != nil {
			log.Printf("template fetch error: %v", err)
			continue
		}

		fp := fmt.Sprintf(
			"%s|%s|%s|%s|%d",
			tpl.PreviousHash,
			tpl.TxRoot,
			tpl.StateRoot,
			tpl.PoWDiffReference.Nbits,
			tpl.Height,
		)

		if fp == lastFP {
			continue
		}
		lastFP = fp
		nextID++
		job, err := buildJob(tpl, nextID)
		if err != nil {
			log.Printf("build job error: %v", err)
			continue
		}

		jobMu.Lock()
		lastJob = currentJob
		lastJobTs = time.Now()
		currentJob = job
		jobMu.Unlock()
		log.Printf("new job -> broadcasting to miners, cnt : %v", len(clients))
		broadcastNotify(job)
	}
}

func handleSubscribe(client *Client, id interface{}) {
	client.mu.Lock()
	client.subscribed = true
	client.mu.Unlock()
	response := StratumResponse{
		ID: id,
		Result: []interface{}{
			nil, magicNum, 8,
		},
		Error: nil,
	}

	sendResponse(client.conn, response)
}

func handleAuthorize(client *Client, id interface{}, params []interface{}) {
	if len(params) < 2 {
		sendErrorResponse(client.conn, id, -32602, "Invalid params")
		return
	}

	username, ok := params[0].(string)
	password, ok2 := params[1].(string)
	if !ok || !ok2 {
		sendErrorResponse(client.conn, id, -32602, "Invalid params")
		return
	}
	usersMu.Lock()
	u, o := users[username]
	usersMu.Unlock()
	if !o || u.Password != password {
		sendErrorResponse(
			client.conn,
			id,
			-32007,
			"Invalid username or password",
		)
		return
	}
	if u.MinerAddr == "" {
		sendErrorResponse(
			client.conn,
			id,
			-32008,
			"Miner address not set",
		)
		return
	}

	client.mu.Lock()
	client.authorized = true
	client.MinerAddr = u.MinerAddr
	client.mu.Unlock()
	sendResponse(client.conn, StratumResponse{
		ID:     id,
		Result: true,
		Error:  nil,
	})
	client.mu.Lock()
	subscribed := client.subscribed
	client.mu.Unlock()
	if subscribed {
		jobMu.RLock()
		job := currentJob
		jobMu.RUnlock()
		if job != nil {
			setMsg := StratumMessage{
				ID:     nil,
				Method: "mining.set_target",
				Params: []interface{}{job.ShareTarget},
			}
			bSet, _ := json.Marshal(setMsg)
			client.conn.Write(append(bSet, '\n'))

			// send notify
			notifyMsg := StratumMessage{
				ID:     nil,
				Method: "mining.notify",
				Params: []interface{}{
					strconv.FormatUint(job.ID, 10),
					job.HeaderHex,
					true,
				},
			}
			bNotify, _ := json.Marshal(notifyMsg)
			client.conn.Write(append(bNotify, '\n'))
		}
	}
}

func submitBlockHeader(fullHeaderHex string, extraNonce2 uint64) (interface{}, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "submitBlockHeader",
		"params":  []interface{}{fullHeaderHex, extraNonce2},
	}
	buf, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", rpcURL, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(rpcUser, rpcPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result interface{} `json:"result"`
		Error  interface{} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %v", rpcResp.Error)
	}

	return rpcResp.Result, nil
}
func sendToAddressRPC(to string, amountEth float64) (string, error) {
	// A) unique nonce
	nonceMu.Lock()
	n := nextNonce
	nextNonce++
	nonceMu.Unlock()

	// B) build tx
	wei := new(big.Int).Mul(
		big.NewInt(int64(amountEth*1e6)), // amt*1e6
		big.NewInt(1e12),                 // *1e12 = amt*1e18
	)
	toAddr := common.HexToAddress(to)

	gasPrice, err := ethClient.SuggestGasPrice(context.Background())
	if err != nil {
		return "", fmt.Errorf("suggest gas price: %w", err)
	}
	tx := types.NewTransaction(n, toAddr, wei, 21000, gasPrice, nil)

	// C) sign EIP-155
	chainID, err := ethClient.NetworkID(context.Background())
	if err != nil {
		return "", fmt.Errorf("fetch chain ID: %w", err)
	}
	signed, err := types.SignTx(tx, types.NewEIP155Signer(chainID), senderKey)
	if err != nil {
		return "", fmt.Errorf("sign tx: %w", err)
	}

	// D) broadcast
	if err := ethClient.SendTransaction(context.Background(), signed); err != nil {
		return "", fmt.Errorf("send tx: %w", err)
	}
	hash := signed.Hash().Hex()
	log.Printf("▶ broadcasted tx %s; waiting…", hash)

	// E) wait for mining
	for {
		receipt, err := ethClient.TransactionReceipt(context.Background(), signed.Hash())
		if err != nil {
			if err == ethereum.NotFound {
				time.Sleep(300 * time.Millisecond)
				continue
			}
			return "", fmt.Errorf("receipt error: %w", err)
		}
		if receipt.Status != 1 {
			return "", fmt.Errorf("tx %s failed in block", hash)
		}
		break
	}

	log.Printf("✅ tx %s mined", hash)
	return hash, nil
}

func distributeRewards(job *Job) {
	clientsMu.RLock()
	defer clientsMu.RUnlock()

	totalEth := float64(job.Reward) / 1e9
	netEth := totalEth * (1 - float64(poolFeePct)/100)

	var totalShares uint64
	for c := range clients {
		c.mu.Lock()
		totalShares += c.shareCounts[job.ID]
		c.mu.Unlock()
	}

	for c := range clients {
		c.mu.Lock()
		shares := c.shareCounts[job.ID]
		addr := c.MinerAddr
		c.mu.Unlock()
		if shares == 0 {
			continue
		}
		amount := netEth * float64(shares) / float64(totalShares)
		payoutQueue <- Payout{JobID: job.ID, Addr: addr, Amount: amount}
		log.Printf("enqueued payout %.8f ETH to %s (shares %d/%d)",
			amount, addr, shares, totalShares)
	}
}

func handleSubmit(client *Client, id interface{}, params []interface{}) {
	if !client.subscribed || !client.authorized {
		sendErrorResponse(client.conn, id, -32000, "Not subscribed or authorized")
		return
	}

	if len(params) < 3 {
		sendErrorResponse(client.conn, id, -32006, "Invalid params")
		return
	}

	jobMu.RLock()
	job := currentJob
	jobMu.RUnlock()
	if job == nil {
		sendErrorResponse(client.conn, id, -32004, "No template yet")
		return
	}

	log.Printf("Received share submission: %+v", params)
	username, _ := params[0].(string)
	jobid, _ := params[1].(string)
	nonce, _ := params[2].(string)
	fmt.Printf("username %s jobid %s nonce %s\n", username, jobid, nonce)

	// new a BLAKE2s hash
	hash1, err := blake2sext.New256(nil)
	if err != nil {
		sendErrorResponse(client.conn, id, -32001, "hash error happen in server")
		return
	}

	task := job.HeaderHex
	Id := job.ID
	client.mu.Lock()
	isCurrent := (strconv.FormatUint(Id, 10) == jobid)
	client.mu.Unlock()
	accetedTask := false
	if !isCurrent {
		if lastJob != nil && jobid == strconv.FormatUint(lastJob.ID, 10) &&
			time.Since(lastJobTs) <= 150*time.Millisecond {
			job = lastJob
			task = job.HeaderHex
			Id = job.ID
			accetedTask = true
		} else {
			sendErrorResponse(client.conn, id, -32005, "Stale template")
			return
		}
	}
	hexString := task
	hexString += nonce
	hexString += magicNum
	byteSlice, err := hex.DecodeString(hexString)
	if err != nil {
		sendErrorResponse(client.conn, id, -32002, "invalid format of nonce, not little endian 64 bit hex string")
		return
	}
	_, err = hash1.Write(byteSlice)
	if err != nil {
		sendErrorResponse(client.conn, id, -32003, "hash error happen in server")
		return
	}
	result := hash1.Sum(nil)
	resultHex := hex.EncodeToString(result)
	// inverse the hash result
	for i := 0; i < 16; i++ {
		var temp = result[i]
		result[i] = result[31-i]
		result[31-i] = temp
	}
	var respRes = false
	var validShare = false
	var respMsg = ""
	// change the hash result to hex string
	resultHex = hex.EncodeToString(result)
	hashoutNum := new(big.Int)
	// change the hex string to big.Int
	_, success := hashoutNum.SetString(resultHex, 16)
	if success {
		shareNum := new(big.Int)
		_, success2 := shareNum.SetString(job.ShareTarget, 16)
		if success2 {
			res2 := hashoutNum.Cmp(shareNum)
			if res2 < 0 {
				client.mu.Lock()
				client.shareCounts[job.ID] += 1
				client.mu.Unlock()
				//valid share
				targetNum := new(big.Int)
				_, success3 := targetNum.SetString(job.Target, 16)
				if success3 {
					res := hashoutNum.Cmp(targetNum)
					if res < 0 {
						//fmt.Println("compare success")
						respRes = true
					} else {
						// fmt.Println("compare failed")
						fmt.Println("compare failed, share is valid but not a block")
						validShare = true
					}
				} else {
					fmt.Println("change target to big number failed please check the input string")
				}
			} else {
				respMsg = "too low difficulty share"
			}
		} else {
			fmt.Println("change target to big number failed please check the input string")
		}
	} else {
		fmt.Println("change hash result to big number failed please check the input string")
	}

	if respRes && !accetedTask {
		fmt.Println("YES, valid share found, submitting block header...")
		headerHex := job.HeaderHex
		extraHex := "090000000000"
		fullHeaderHex := headerHex[:144*2] + extraHex + nonce + magicNum

		extra2Num, _ := strconv.ParseUint(strings.TrimPrefix("0x00", "0x"), 16, 64)
		result, _ := submitBlockHeader(fullHeaderHex, extra2Num)
		time.Sleep(150 * time.Millisecond)
		log.Printf("▶ submitted block header, %+v node replied: %+v", job.ID, result)
		go distributeRewards(job)
	}

	sendResponse(client.conn, StratumResponse{
		ID:     id,
		Result: respRes || validShare,
		Error:  respMsg,
	})
	fmt.Println("#########################################################################################################")
}

func sendResponse(conn net.Conn, response StratumResponse) {
	log.Printf("sendResponse: %+v", response)
	respJSON, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling JSON response: %v", err)
		return
	}
	respJSON = append(respJSON, '\n')
	_, err = conn.Write(respJSON)
	if err != nil {
		log.Printf("Error writing response to client: %v", err)
	}
}

func sendErrorResponse(conn net.Conn, id interface{}, code int, message string) {
	response := StratumResponse{
		ID:     id,
		Result: nil,
		Error:  []interface{}{code, message, nil},
	}
	sendResponse(conn, response)
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/api/signup", signupHandler)
	http.HandleFunc("/api/login", loginHandler)
	http.HandleFunc("/api/addMiner", addMinerHandler)
	http.HandleFunc("/api/getMiner", getMinerHandler)

	go func() {
		fmt.Println("API server running on :8080")
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	go (&templateFetcher{}).Start()

	listener, err := net.Listen("tcp", ":3334")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	fmt.Println("Stratum server is listening on port 3334")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go handleConnection(conn)
	}
}
