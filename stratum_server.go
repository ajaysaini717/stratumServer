package main

import (
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

const rpcURL = "http://172.16.15.105:38131"
const rpcUser = "test"
const rpcPassword = "test"

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
	defer resp.Body.Close()

	fmt.Printf("Response : %+v and error %+v\n",resp,err)
	var rpcResp struct {
		Result BlockTemplate `json:"result"`
		Error  interface{}   `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, err
	}
	fmt.Printf("Actual Error : %+v",rpcResp.Error)
	if rpcResp.Error != nil {
		return nil, errors.New("RPC error in getBlockTemplate")
	}
    // fmt.Println(rpcResp.Result)
	return &rpcResp.Result, nil
}

// Client 定义客户端结构体
type Client struct {
	conn       net.Conn
	authorized bool
	subscribed bool
	mu         sync.Mutex
	id         uint64
	disCh chan struct{}
}

var headerMap = struct {
    sync.RWMutex
    m map[string]string
}{m : make(map[string]string)}

// only 32 bytes target in big endian hex

// var testTarget = "0001000000000000000000000000000000000000000000000000000000000000"
// var testTarget = "0000000000400000000000000000000000000000000000000000000000000000" 
				//   0000000100000000000000000000000000000000000000000000000000000000    // diff 1024
				  
var testTarget = "000198f200000000000000000000000000000000000000000000000000000000" // diff 256


// all other data such as task and magicNum in little endian hex
// var testTask = "00000000000000004fd7c33d212c06007e3a5ee18306b2ff33d82f34bdbb429c5330a902e7db0286a43fd33999bd5dec0300040000009d3f482df44b11a178c6b51209d262d32b77a0f48d89f7c21bd9e4cef1b90e7c06000000b597e466f2d46b790ae4352ab2270e388fc96447c59828a839a222e0c56532dc07000000483d2991e9ead2ff19f7c0eaae8781cd600cbeee58361aaa"
var testTask = "00004fd7c33d212c06007e3a5ee18306b2ff33d82f34bdbb429c5330a902e7db0286a43fd33999bd5dec0300040000009d3f482df44b11a178c6b51209d262d32b77a0f48d89f7c21bd9e4cef1b90e7c06000000b597e466f2d46b790ae4352ab2270e388fc96447c59828a839a222e0c56532dc07000000483d2991e9ead2ff19f7c0eaae8781cd600cbeee58361aaa"
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
	fmt.Printf("\n\nSending notification called for jobId %v\n\n",client.id)
	if !client.subscribed || !client.authorized {
		return nil
	}
	// task := str + testTask
	// fmt.Println(task)
	tpl, err := getBlockTemplate()
    // fmt.Printf("template is ===> %+v\n",tpl)
	if err != nil {
        log.Printf("Error fetching blocktemplate: %v", err)
		return err
	}
	header := make([]byte, 144)
	off := 0
	binary.LittleEndian.PutUint32(header[off:], uint32(tpl.Version))
	off += 4
    
	parent, _ := hex.DecodeString(strings.TrimPrefix(tpl.Parents[0].Data, "0x"))
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
	emptyBytes := make([]byte, 6)
	header = append(header, emptyBytes...)
	// diff := uint32(nbits)
	// target := CompactToBig(diff)
	// fmt.Printf("Target : %v %v\n",target,diff)
	// sendSetTargetMessage(client,target.String())
	// nonce := make([]byte, 8)
	// header = append(header, nonce...)

	// magic := make([]byte, 2)
	// binary.LittleEndian.PutUint16(magic, 0x0045)
	// header = append(header, magic...)

	headerHex := hex.EncodeToString(header)
    
	// 3) build the 160-byte task: [header][6B extra-nonce placeholder][8B nonce placeholder][2B magic]
	// extra2Placeholder := strings.Repeat("00", 6)
	// noncePlaceholder := strings.Repeat("00", 8)
	// magicNum := "1122"
    client.id += 1
	task := headerHex
    // fmt.Printf("headerhex while sending %v",headerHex)
    str := fmt.Sprintf("%012x", client.id)
    headerMap.Lock()
    // fmt.Printf("Setting headehex %v\n",str)
    headerMap.m[str] = headerHex
	// fmt.Printf("SIZE : %+v",len(headerHex))
    headerMap.Unlock()
    // fmt.Printf("\n\nTask : %v and id %v\n\n",task,strconv.FormatUint(client.id, 10))
	fmt.Println("Sending template !!")
	comBits := CompactToBig(uint32(nbits)).Text(16)
	target := fmt.Sprintf("%64s",comBits)
	target = strings.ReplaceAll(target, " ", "0")
	fmt.Printf("Target , %v difficulty %v\n: ",target,nbits)
	sendSetTargetMessage(client, target)

	msg := StratumMessage{
		ID:     nil,
		Method: "mining.notify",
		Params: []interface{}{
			strconv.FormatUint(client.id, 10),
			task, //testTask,
			true,
		},
	}

	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling mining.notify message: %v", err)
		return err
	}
	msgJSON = append(msgJSON, '\n')
	// fmt.Printf("writing to the channel in notify.%v %v\n",client.id,len(header))
	_, err = client.conn.Write(msgJSON)
	if err != nil {
		log.Printf("Error sending mining.notify message: %v", err)
		return err
	}
	// fmt.Printf("writing to the channel out notify.%v\n",client.id)
	return nil
}

// 每 5 秒发送一次 mining.notify 消息
func sendPeriodicNotify(client *Client) {
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// fmt.Println("TICK TICK TICK ... ")
			start := time.Now()
			if err := sendNotifyMessage(client);err != nil {
				return
			}
			elapsed := time.Since(start)

			if elapsed > 10 * time.Millisecond {
				log.Printf("sendNotifyMessage took %s", elapsed)
			}
		case <- client.disCh:
			return
		}
	}
}

// 处理客户端连接
func handleConnection(conn net.Conn) {
	// fmt.Println("A")
	client := &Client{
		conn:       conn,
		authorized: false,
		subscribed: false,
		id:         0,
		disCh: make(chan struct{}),
	}
	defer func() {
		close(client.disCh)
		conn.Close()
	}()

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Error reading from client: %v", err)
			return
		}

		var msg StratumMessage
		// fmt.Println("Buffer : ",buf)
		err = json.Unmarshal(buf[:n], &msg)
		if err != nil {
			log.Printf("Error unmarshaling JSON: %v", err)
			sendErrorResponse(conn, msg.ID, -32700, "Parse error")
			continue
		}

		log.Printf("Received message: %+v", msg)

		switch msg.Method {
		case "mining.subscribe":
			handleSubscribe(client, msg.ID)
			if client.subscribed && client.authorized {
				// fmt.Println("sendNotifyMessage called ")
				sendNotifyMessage(client)
				go sendPeriodicNotify(client)
			}
		case "mining.authorize":
			handleAuthorize(client, msg.ID, msg.Params)
			if client.subscribed && client.authorized {
				// fmt.Println("sendNotifyMessage called ")
				sendNotifyMessage(client)
				go sendPeriodicNotify(client)
			}
		case "mining.submit":
			handleSubmit(client, msg.ID, msg.Params)
		default:
			sendErrorResponse(conn, msg.ID, -32601, "Method not found")
		}
	}
}

func sendSetTargetMessage(client *Client, target string) {
	/*if!client.subscribed ||!client.authorized {
	    return
	}*/

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

	/*client.mu.Lock()
	  client.target = target
	  client.mu.Unlock()*/
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

	// 模拟订阅响应
	/*response := StratumResponse{
	    ID: id,
	    Result: []interface{}{
	        []string{"mining.notify", "ae6812eb4cd7735a302a8a9dd95cf71f"},
	        []string{"00000000000000000000000000000000", "ffffffffffffffffffffffffffffffff"},
	        4,
	    },
	    Error: nil,
	}*/
	response := StratumResponse{
		ID: id,
		Result: []interface{}{
			nil, magicNum, 8,
		},
		Error: nil,
	}

	sendResponse(client.conn, response)
	// fmt.Println("Setting diff i !")
	// sendSetTargetMessage(client, testTarget)
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

func toLittleEndianHex(beHex string) (string, error) {
    b, err := hex.DecodeString(beHex)
    if err != nil {
        return "", fmt.Errorf("invalid hex: %w", err)
    }
    if len(b) != 8 {
        return "", fmt.Errorf("expected 8 bytes, got %d", len(b))
    }
    for i := 0; i < len(b)/2; i++ {
        b[i], b[len(b)-1-i] = b[len(b)-1-i], b[i]
    }
    return hex.EncodeToString(b), nil
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
	// hash, err := blake2sext.New256(nil)
	hash1, err := blake2sext.New256(nil)
	if err != nil {
		// fmt.Println("Error creating Blake2s hash:", err)
		sendErrorResponse(client.conn, id, -32001, "something error happen in server")
		return
	}
	
	decimalUint, err := strconv.ParseUint(jobid, 10, 64) 
    // fmt.Println(decimalUint)
	if err != nil {
		fmt.Printf("str to int failed %v\n", err)
	}
	//hexString := testTask
	str := fmt.Sprintf("%012x", decimalUint)
    headerMap.RLock()
    // fmt.Printf("Task in submit jobid : %v\n",jobid)
    task,ok := headerMap.m[str]
    headerMap.RUnlock()
    if !ok {
        sendErrorResponse(client.conn, id, -32003, "Unknown jobid")
    }
	// fmt.Println("task is ===> ",task , "job id ",jobid)
    // fmt.Printf("Task in submit %v\n",task)
	hexString := task
	//resultStr := "17cbeee16d478d739bbf3a41e6e0ef10aa5ad0d9d97b9e3cf98c470000000000"
	hexString += nonce
	hexString += magicNum
	// fmt.Printf("hexstring : %+v\n",hexString)
    // fmt.Printf("Hex string in submit : %v",hexString)
	byteSlice, err := hex.DecodeString(hexString)
	// fmt.Printf("ByteSice %+v %+v\n",byteSlice,len(byteSlice))
	if err != nil {
		//fmt.Printf("decode hex string error: %v\n", err)
		sendErrorResponse(client.conn, id, -32002, "invalid format of nonce, not little endian 64 bit hex string")
		return
	}
	// _, err = hash.Write(byteSlice)
	_,err = hash1.Write(byteSlice)
	if err != nil {
		//fmt.Println("Error writing to Blake2s hash:", err)
		sendErrorResponse(client.conn, id, -32003, "hash error happen in server")
		return
	}
	// compute hash
	// result := hash.Sum(nil)
	result := hash1.Sum(nil)
	// fmt.Printf("Result b : %+v\n",result)
	resultHex := hex.EncodeToString(result)
	// fmt.Println("HASHSHSHSHSHSHSHS BEFOREOOOO \n",resultHex)
	// inverse the hash result
	for i := 0; i < 16; i++ {
		var temp = result[i]
		result[i] = result[31-i]
		result[31-i] = temp
	}
	// fmt.Printf("Result a : %+v\n",result)
	var respRes = false
	var respMsg = ""
	// change the hash result to hex string
	resultHex = hex.EncodeToString(result)
	// fmt.Printf("Blake2s hash input\n%s\nresult: %s\n", hexString, resultHex)
	hashoutNum := new(big.Int)
	// change the hex string to big.Int
	_, success := hashoutNum.SetString(resultHex, 16)
	// fmt.Printf("Hex : %+v hashOutNum %+v\n",resultHex,hashoutNum)
	if success {
		targetNum := new(big.Int)
		_, success2 := targetNum.SetString(testTarget, 16)
		if success2 {
			res := hashoutNum.Cmp(targetNum)

			if res < 0 {
				//fmt.Println("compare success")
				respRes = true
			} else {
				// fmt.Println("compare failed")
				respMsg = "low difficulty share"
			}
			} else {
				fmt.Println("change target to big number failed please check the input string")
			}
	} else {
		fmt.Println("change hash result to big number failed please check the input string")
	}
	
    if respRes {
		// sendNotifyMessage(client)
		// fmt.Println("All good under target !!")
        // 1) pull back the 144-byte headerHex you cached
        headerMap.RLock()
        headerHex, _ := headerMap.m[str]
        headerMap.RUnlock()

        // 2) build fullHeaderHex = headerHex + "0x08" + nonce
        extraHex := "09"
		// nonceraw,err := toLittleEndianHex(nonce)
		if err != nil {
			log.Fatalf("Failed to convert nonce : %v",err)
		}
	
		// fmt.Printf("Length %v and actual %v\n",nonceraw,nonce)
        fullHeaderHex := headerHex[:144*2] + extraHex + nonce

        // 3) parse the two numbers
        extra2Num, _ := strconv.ParseUint(strings.TrimPrefix("0x00", "0x"), 16, 64)
        // nonceNum, _  := strconv.ParseUint(nonce, 16, 64)

        // fmt.Printf("full header ==> %+v",fullHeaderHex)
        // 4) fire off RPC
		
		// fmt.Println("sendNotifyMessage called ")
		// if err := sendNotifyMessage(client);err != nil {
		// 	return
		// }
        result, err := submitBlockHeader(fullHeaderHex, extra2Num)
        if err != nil {
            // if upstream submit failed, tell the miner
            sendResponse(client.conn, StratumResponse{
                ID:     id,
                Result: false,
                Error:  fmt.Sprintf("submitBlockHeader error: %v", err),
            })
            return
        }
        log.Printf("▶ submitted block header, %+v node replied: %+v",client.id,result)
    }

	sendResponse(client.conn, StratumResponse{
		ID:     id,
		Result: respRes,
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
	listener, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	fmt.Println("Stratum server is listening on port 3333...")

	for {
		// 接受客户端连接
		// fmt.Println("Handling connection !")
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		// 处理客户端连接
		go handleConnection(conn)
	}
}