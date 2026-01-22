package wallet

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"strings"

	"encoding/base64"
	"net/url"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// Client defines the interface for wallet operations
type Client interface {
	SendTransaction(ctx context.Context, toAddr string, amountWei *big.Int) (string, error)
	GetBalance(ctx context.Context) (*big.Int, error)
	Close()
}

// EthWalletClient implements Client using standard EVM RPC with Local Signing
type EthWalletClient struct {
	client     *ethclient.Client
	rpcClient  *rpc.Client
	privateKey *ecdsa.PrivateKey
	fromAddr   common.Address
	chainID    *big.Int
}

func (w *EthWalletClient) GetBalance(ctx context.Context) (*big.Int, error) {
	if w.client == nil {
		return nil, fmt.Errorf("client not connected")
	}
	return w.client.BalanceAt(ctx, w.fromAddr, nil)
}

func NewEthWalletClient(ctx context.Context, rpcURL string, privateKeyHex string) (*EthWalletClient, error) {
	// Parse URL to check for auth
	u, err := url.Parse(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("invalid RPC URL: %w", err)
	}

	var rpcClient *rpc.Client

	// If User info is present, use rpc.WithHeader
	if u.User != nil {
		password, _ := u.User.Password()
		auth := u.User.Username() + ":" + password
		basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))

		// Strip user info from URL for the connection (optional, but cleaner)
		u.User = nil
		cleanURL := u.String()

		rpcClient, err = rpc.DialOptions(ctx, cleanURL, rpc.WithHeader("Authorization", basicAuth))
	} else {
		rpcClient, err = rpc.DialContext(ctx, rpcURL)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC: %w", err)
	}

	client := ethclient.NewClient(rpcClient)

	// Parse Private Key
	if strings.HasPrefix(privateKeyHex, "0x") {
		privateKeyHex = privateKeyHex[2:]
	}
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("error casting public key to ECDSA")
	}

	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Get ChainID
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	return &EthWalletClient{
		client:     client,
		rpcClient:  rpcClient,
		privateKey: privateKey,
		fromAddr:   fromAddress,
		chainID:    chainID,
	}, nil
}

func (w *EthWalletClient) Close() {
	if w.client != nil {
		w.client.Close()
	}
}

// SendTransaction signs and sends a raw transaction
func (w *EthWalletClient) SendTransaction(ctx context.Context, toAddr string, amountWei *big.Int) (string, error) {
	if !common.IsHexAddress(toAddr) {
		return "", fmt.Errorf("invalid hex address: %s", toAddr)
	}
	to := common.HexToAddress(toAddr)

	// 1. Get Nonce
	nonce, err := w.client.PendingNonceAt(ctx, w.fromAddr)
	if err != nil {
		return "", fmt.Errorf("failed to get nonce: %w", err)
	}

	// 2. Get Gas Price
	gasPrice, err := w.client.SuggestGasPrice(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get gas price: %w", err)
	}

	// 3. Estimate Gas (Optional but recommended)
	gasLimit := uint64(21000) // Standard transfer
	txCost := new(big.Int).Mul(gasPrice, big.NewInt(int64(gasLimit)))
	totalReq := new(big.Int).Add(amountWei, txCost)

	log.Printf("[Payout Debug] Address=%s Amount=%s Wei, GasPrice=%s, Limit=%d, TxCost=%s, TotalReq=%s",
		toAddr, amountWei.String(), gasPrice.String(), gasLimit, txCost.String(), totalReq.String())

	// 4. Create Transaction
	tx := types.NewTransaction(nonce, to, amountWei, gasLimit, gasPrice, nil)

	// 5. Sign Transaction
	signer := types.LatestSignerForChainID(w.chainID)
	signedTx, err := types.SignTx(tx, signer, w.privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign tx: %w", err)
	}

	// 6. Broadcast
	err = w.client.SendTransaction(ctx, signedTx)
	if err != nil {
		return "", fmt.Errorf("failed to send tx: %w: balance %s, queued cost %s, tx cost %s, overshot %s",
			err, "N/A", "N/A", totalReq.String(), "N/A") // Partial error wrap to match user logs structure if possible
	}

	return signedTx.Hash().Hex(), nil
}
