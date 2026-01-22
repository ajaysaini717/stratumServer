package main

import (
	"asicPool/internal/pool/server"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func main() {

	if err := godotenv.Load(); err != nil {
		log.Printf("load env: %v", err)
	} else {
		log.Println("env file found and loaded")
	}
	listenAddr := "0.0.0.0:" + os.Getenv("POOL_PORT")
	rpcURL := os.Getenv("NODE_RPC_URL")
	rpcUser := os.Getenv("NODE_RPC_USER")
	rpcPassword := os.Getenv("NODE_RPC_PASS")
	pplnsWorkStr := os.Getenv("PPLNS_N_WORK")
	var pplnsWork float64
	if pplnsWorkStr != "" {
		if val, err := strconv.ParseFloat(pplnsWorkStr, 64); err == nil {
			pplnsWork = val
		} else {
			log.Printf("Invalid PPLNS_N_WORK: %v", err)
		}
	}

	// Parse Maturity Config
	blockMaturity := 0 // default handled in server
	if val := os.Getenv("POOL_BLOCK_MATURITY"); val != "" {
		if x, err := strconv.Atoi(val); err == nil {
			blockMaturity = x
		}
	}
	payoutMaturity := 0 // default handled in server
	if val := os.Getenv("POOL_PAYOUT_MATURITY"); val != "" {
		if x, err := strconv.Atoi(val); err == nil {
			payoutMaturity = x
		}
	}

	poolFee := 0.0
	if val := os.Getenv("POOL_FEE_PERCENTAGE"); val != "" {
		if x, err := strconv.ParseFloat(val, 64); err == nil {
			poolFee = x
		}
	}

	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		log.Println("⚠️ PG_URL not set, database persistence disabled")
	}

	if listenAddr == "" {
		listenAddr = ":3334"
	}
	if rpcURL == "" {
		rpcURL = "http://127.0.0.1:38131"
	}
	walletRPCURL := os.Getenv("WALLET_RPC_URL")
	if walletRPCURL == "" {
		// Fallback or just empty
		log.Println("Note: WALLET_RPC_URL not set, checking RPC_URL or defaulting")
	}

	if rpcUser == "" {
		rpcUser = "test"
	}
	if rpcPassword == "" {
		rpcPassword = "test"
	}
	poolPrivateKey := os.Getenv("POOL_PRIVATE_KEY")
	ipfsNodeURL := os.Getenv("IPFS_NODE_URL")

	cfg := server.Config{
		ListenAddr:          listenAddr,
		RPCURL:              rpcURL,
		WalletRPCURL:        walletRPCURL,
		RPCUser:             rpcUser,
		RPCPassword:         rpcPassword,
		PPLNSTargetWork:     pplnsWork,
		BlockMaturityDepth:  blockMaturity,
		PayoutMaturityDepth: payoutMaturity,
		PoolFeePercentage:   poolFee,
		PGURL:               pgURL,
		PoolPrivateKey:      poolPrivateKey,
		IPFSNodeURL:         ipfsNodeURL,
		LogRotationInterval: os.Getenv("LOG_ROTATION_INTERVAL"),
	}
	if err := server.Run(cfg); err != nil {
		log.Fatalf("pool server failed: %v", err)
	}
}
