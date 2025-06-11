package main

import (
	"bufio"
	"bytes"
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

	// "golang.org/x/crypto/blake2s"
	"golang.org/x/crypto/blake2sext"
)

// StratumMessage 定义 Stratum 消息结构体
type StratumMessage struct {
	ID     interface{}   `json:"id"`
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

// StratumResponse 定义 Stratum 响应结构体
type StratumResponse struct {
	ID     interface{} `json:"id"`
	Result interface{} `json:"result"`
	Error  interface{} `json:"error"`
}

var (
	rpcURL      = "http://172.16.15.105:38131"
	rpcUser     = "test"
	rpcPassword = "test"
	poolMu      sync.Mutex
	poolClients = make(map[*Client]struct{})
	shareFactor = 2
	// blockMu     sync.Mutex
	// blockmined  bool = false
)

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

func getBlockTemplate() (*BlockTemplate, error) {
	// build request
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

	// fmt.Printf("Response : %+v and error %+v\n",resp,err)
	var rpcResp struct {
		Result BlockTemplate `json:"result"`
		Error  interface{}   `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, err
	}
	// fmt.Printf("Actual Error : %+v",rpcResp.Error)
	if rpcResp.Error != nil {
		fmt.Printf("ERORROR : %+v", rpcResp.Error)
		return nil, errors.New("RPC error in getBlockTemplate")
	}
	// fmt.Println(rpcResp.Result)
	return &rpcResp.Result, nil
}

// Client 定义客户端结构体
type Client struct {
	conn          net.Conn
	authorized    bool
	subscribed    bool
	mu            sync.Mutex
	id            uint64
	disCh         chan struct{}
	shareCount    uint64
	testTarget    string
	shareTarget   string
	currentHeader string
}

var headerMap = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

var magicNum = "2211"

func CompactToBig(compact uint32) *big.Int {
	// Extract the mantissa, sign bit, and exponent.
	mantissa := compact & 0x007fffff
	isNegative := compact&0x00800000 != 0
	exponent := uint(compact >> 24)

	// Since the base for the exponent is 256, the exponent can be treated
	// as the number of bytes to represent the full 256-bit number.  So,
	// treat the exponent as the number of bytes and shift the mantissa
	// right or left accordingly.  This is equivalent to:
	// N = mantissa * 256^(exponent-3)
	var bn *big.Int
	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		bn = big.NewInt(int64(mantissa))
	} else {
		bn = big.NewInt(int64(mantissa))
		bn.Lsh(bn, 8*(exponent-3))
	}

	// Make it negative if the sign bit is set.
	if isNegative {
		bn = bn.Neg(bn)
	}

	return bn
}

// 发送 mining.notify 消息
func sendNotifyMessage(client *Client) error {
	// fmt.Printf("\n\nSending notification called for jobId %v\n\n",client.id)
	if !client.subscribed || !client.authorized {
		return nil
	}
	// task := str + testTask
	// fmt.Println(task)
	tpl, err := getBlockTemplate()
	// fmt.Printf("tpl is ===> %+v",tpl)
	// fmt.Printf("template is ===> %+v\n",tpl)
	if err != nil {
		log.Printf("Error fetching blocktemplate: %v", err)
		return err
	}
	header := make([]byte, 144)
	off := 0
	binary.LittleEndian.PutUint32(header[off:], uint32(tpl.Version))
	off += 4

	parent, _ := hex.DecodeString(strings.TrimPrefix(tpl.PreviousHash, "0x"))
	copy(header[off:], parent)
	off += 32

	txroot, _ := hex.DecodeString(strings.TrimPrefix(tpl.TxRoot, "0x"))
	copy(header[off:], txroot)
	off += 32

	stateroot, _ := hex.DecodeString(strings.TrimPrefix(tpl.StateRoot, "0x"))
	copy(header[off:], stateroot)
	off += 32

	nbits, _ := strconv.ParseUint(tpl.PoWDiffReference.Nbits, 16, 32)

	binary.LittleEndian.PutUint32(header[off:], uint32(nbits))
	off += 4

	binary.LittleEndian.PutUint32(header[off:], uint32(tpl.CurTime))
	off += 4
	binary.LittleEndian.PutUint64(header[off:], uint64(tpl.Height))
	off += 8

	cbAddr, _ := hex.DecodeString(strings.TrimPrefix(tpl.CoinbaseAddress, "0x"))
	copy(header[off:], cbAddr)
	off += 20

	binary.LittleEndian.PutUint64(header[off:], tpl.Reward)
	off += 8

	if off != 144 {
		log.Printf("Warning: header size %d != 144", off)
	}
	emptyBytes := []byte{9, 0, 0, 0, 0, 0}

	header = append(header, emptyBytes...)

	headerHex := hex.EncodeToString(header)
	client.id += 1
	task := headerHex
	str := fmt.Sprintf("%012x", client.id)
	headerMap.Lock()
	headerMap.m[str] = headerHex
	headerMap.Unlock()
	client.mu.Lock()
	client.currentHeader = headerHex
	client.mu.Unlock()
	fmt.Printf("Sending template ==> \n")
	comBits := CompactToBig(uint32(nbits)).Text(16)
	target := fmt.Sprintf("%64s", comBits)
	target = strings.ReplaceAll(target, " ", "0")
	fmt.Printf("target %v\n ", target)

	//NEED TO BE TESTED
	comBitsShare := new(big.Int).Mul(CompactToBig(uint32(nbits)), big.NewInt(int64(shareFactor))).Text(16)
	sharetarget := fmt.Sprintf("%64s", comBitsShare)
	sharetarget = strings.ReplaceAll(sharetarget, " ", "0")
	fmt.Printf("share target %v\n ", sharetarget)
	//END OF TESTING

	client.testTarget = target
	client.shareTarget = sharetarget
	sendSetTargetMessage(client, client.shareTarget)

	msg := StratumMessage{
		ID:     nil,
		Method: "mining.notify",
		Params: []interface{}{
			strconv.FormatUint(client.id, 10),
			task,
			true,
		},
	}

	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling mining.notify message: %v", err)
		return err
	}
	msgJSON = append(msgJSON, '\n')
	_, err = client.conn.Write(msgJSON)
	if err != nil {
		log.Printf("Error sending mining.notify message: %v", err)
		return err
	}
	// blockMu.Lock()
	// blockmined = false
	// blockMu.Unlock()
	return nil
}

// 处理客户端连接
func handleConnection(conn net.Conn) {
	fmt.Println("YES, new client connected")
	client := &Client{
		conn:        conn,
		authorized:  false,
		subscribed:  false,
		id:          0,
		disCh:       make(chan struct{}),
		shareCount:  0,
		testTarget:  "",
		shareTarget: "",
	}
	poolMu.Lock()
	poolClients[client] = struct{}{}
	poolMu.Unlock()

	defer func() {
		poolMu.Lock()
		delete(poolClients, client)
		poolMu.Unlock()
		close(client.disCh)
		conn.Close()
	}()

	// buf := make([]byte, 4096)
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("Error reading from client: %v", err)
			return
		}

		// Trim whitespace/newlines
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}

		// Unmarshal exactly one JSON object
		var msg StratumMessage
		if err := json.Unmarshal(trimmed, &msg); err != nil {
			log.Printf("Error unmarshaling JSON: %v", err)
			// send JSON-RPC parse error (id=nil)
			sendErrorResponse(conn, nil, -32700, "Parse error")
			continue
		}

		log.Printf("Received message: %+v", msg)

		switch msg.Method {
		case "mining.subscribe":
			handleSubscribe(client, msg.ID)
		case "mining.authorize":
			handleAuthorize(client, msg.ID, msg.Params)
			if client.subscribed && client.authorized {
				startTemplateWatcher(client, 100*time.Millisecond)
			}
		case "mining.submit":
			handleSubmit(client, msg.ID, msg.Params)
		default:
			sendErrorResponse(conn, msg.ID, -32601, "Method not found")
		}
	}
}

func startTemplateWatcher(client *Client, interval time.Duration) {
	var lastTemplateHash string

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-client.disCh:
				return
			case <-ticker.C:
				tpl, err := getBlockTemplate()
				if err != nil {
					log.Printf("Watcher: failed to fetch template: %v", err)
					continue
				}

				// Compose a hash/fingerprint of the block template
				templateHash := fmt.Sprintf("%s-%d-%s", tpl.PreviousHash, tpl.CurTime, tpl.TxRoot)

				// If template is new, notify miner
				if templateHash != lastTemplateHash {
					log.Println("Watcher: new template detected, sending mining.notify...")
					err := sendNotifyMessage(client)
					if err != nil {
						log.Printf("Watcher: failed to send notify: %v", err)
					}
					lastTemplateHash = templateHash
				}
			}
		}
	}()
}

func sendSetTargetMessage(client *Client, target string) {
	msg := StratumMessage{
		ID:     nil,
		Method: "mining.set_target",
		Params: []interface{}{target},
	}

	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling mining.set_target message: %v", err)
		return
	}

	msgJSON = append(msgJSON, '\n')
	_, err = client.conn.Write(msgJSON)
	if err != nil {
		log.Printf("Error sending mining.set_target message: %v", err)
	}
}

// 处理订阅消息
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

// 处理授权消息
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

	// 简单模拟授权逻辑
	if username == "testuser" && password == "testuser" {
		client.mu.Lock()
		client.authorized = true
		client.mu.Unlock()
		sendResponse(client.conn, StratumResponse{
			ID:     id,
			Result: true,
			Error:  nil,
		})
	} else {
		sendResponse(client.conn, StratumResponse{
			ID:     id,
			Result: false,
			Error:  nil,
		})
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

// 处理提交消息
func handleSubmit(client *Client, id interface{}, params []interface{}) {
	if !client.subscribed || !client.authorized {
		sendErrorResponse(client.conn, id, -32000, "Not subscribed or authorized")
		return
	}

	if len(params) < 3 {
		sendErrorResponse(client.conn, id, -32602, "Invalid params")
		return
	}

	// 简单模拟提交处理
	log.Printf("Received share submission: %+v", params)
	username, _ := params[0].(string)
	jobid, _ := params[1].(string)
	nonce, _ := params[2].(string)
	fmt.Printf("username %s jobid %s nonce %s\n", username, jobid, nonce)

	// new a BLAKE2s hash
	hash1, err := blake2sext.New256(nil)
	if err != nil {
		sendErrorResponse(client.conn, id, -32001, "something error happen in server")
		return
	}

	decimalUint, err := strconv.ParseUint(jobid, 10, 64)
	if err != nil {
		fmt.Printf("str to int failed %v\n", err)
	}
	str := fmt.Sprintf("%012x", decimalUint)
	headerMap.RLock()
	task, ok := headerMap.m[str]
	headerMap.RUnlock()
	if !ok {
		sendErrorResponse(client.conn, id, -32003, "Unknown jobid")
		return
	}
	client.mu.Lock()
	isCurrent := (task == client.currentHeader)
	client.mu.Unlock()
	if !isCurrent {
		sendErrorResponse(client.conn, id, -32005, "Stale template")
		return
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
		_, success2 := shareNum.SetString(client.shareTarget, 16)
		if success2 {
			res2 := hashoutNum.Cmp(shareNum)
			if res2 < 0 {
				client.mu.Lock()
				client.shareCount += 1
				client.mu.Unlock()
				//valid share
				targetNum := new(big.Int)
				_, success3 := targetNum.SetString(client.testTarget, 16)
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

	if respRes {
		fmt.Println("YES, valid share found, submitting block header...")
		headerMap.RLock()
		headerHex, _ := headerMap.m[str]
		headerMap.RUnlock()
		extraHex := "090000000000"
		fullHeaderHex := headerHex[:144*2] + extraHex + nonce + magicNum

		// 3) parse the two numbers
		extra2Num, _ := strconv.ParseUint(strings.TrimPrefix("0x00", "0x"), 16, 64)
		// 4) fire off RPC
		result, _ := submitBlockHeader(fullHeaderHex, extra2Num)

		log.Printf("▶ submitted block header, %+v node replied: %+v", client.id, result)
	}

	sendResponse(client.conn, StratumResponse{
		ID:     id,
		Result: respRes || validShare,
		Error:  respMsg,
	})
	fmt.Println("#########################################################################################################")
}

// 发送响应消息
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

// 发送错误响应消息
func sendErrorResponse(conn net.Conn, id interface{}, code int, message string) {
	response := StratumResponse{
		ID:     id,
		Result: nil,
		Error:  []interface{}{code, message, nil},
	}
	sendResponse(conn, response)
}

func main() {
	// 监听指定端口
	listener, err := net.Listen("tcp", ":3336")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	fmt.Println("Stratum server is listening on port 3336")

	for {
		// 接受客户端连接
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		// 处理客户端连接
		go handleConnection(conn)
	}
}
