package main

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"net"
// 	"strconv"
// 	"sync"
// 	"time"

// 	"encoding/hex"
// 	"math/big"

// 	//"golang.org/x/crypto/blake2s"
// 	"golang.org/x/crypto/blake2sext"
// )

// // StratumMessage 定义 Stratum 消息结构体
// type StratumMessage struct {
//     ID      interface{} `json:"id"`
//     Method  string      `json:"method"`
//     Params  []interface{} `json:"params"`
// }

// // StratumResponse 定义 Stratum 响应结构体
// type StratumResponse struct {
//     ID      interface{} `json:"id"`
//     Result  interface{} `json:"result"`
//     Error   interface{} `json:"error"`
// }

// // Client 定义客户端结构体
// type Client struct {
//     conn net.Conn
//     authorized bool
//     subscribed bool
//     mu sync.Mutex
//     id uint64
// }

// // only 32 bytes target in big endian hex
// //var testTarget = "0001000000000000000000000000000000000000000000000000000000000000"
// //var testTarget = "0000000000400000000000000000000000000000000000000000000000000000"     // diff 1024
// var testTarget = "0000000001000000000000000000000000000000000000000000000000000000"     // diff 256

// // all other data such as task and magicNum in little endian hex
// //var testTask = "00000000000000004fd7c33d212c06007e3a5ee18306b2ff33d82f34bdbb429c5330a902e7db0286a43fd33999bd5dec0300040000009d3f482df44b11a178c6b51209d262d32b77a0f48d89f7c21bd9e4cef1b90e7c06000000b597e466f2d46b790ae4352ab2270e388fc96447c59828a839a222e0c56532dc07000000483d2991e9ead2ff19f7c0eaae8781cd600cbeee58361aaa"
// var testTask = "4fd7c33d212c06007e3a5ee18306b2ff33d82f34bdbb429c5330a902e7db0286a43fd33999bd5dec0300040000009d3f482df44b11a178c6b51209d262d32b77a0f48d89f7c21bd9e4cef1b90e7c06000000b597e466f2d46b790ae4352ab2270e388fc96447c59828a839a222e0c56532dc07000000483d2991e9ead2ff19f7c0eaae8781cd600cbeee58361aaa"
// var magicNum = "1122"

// // 发送 mining.notify 消息
// func sendNotifyMessage(client *Client) {
//     fmt.Println("Sending notification ")
//     if!client.subscribed ||!client.authorized {
//         return
//     }
//     client.id += 1
//     str := fmt.Sprintf("%016x", client.id)
//     task := str + testTask
//     fmt.Println(task)
//     msg := StratumMessage{
//         ID: nil,
//         Method: "mining.notify",
//         Params: []interface{}{
//             strconv.FormatUint(client.id, 10),
//             task,   //testTask,
//             true,
//         },
//     }

//     msgJSON, err := json.Marshal(msg)
//     if err != nil {
//         log.Printf("Error marshaling mining.notify message: %v", err)
//         return
//     }
//     msgJSON = append(msgJSON, '\n')
//     _, err = client.conn.Write(msgJSON)
//     if err != nil {
//         log.Printf("Error sending mining.notify message: %v", err)
//     }
// }

// // 每 5 秒发送一次 mining.notify 消息
// func sendPeriodicNotify(client *Client) {
//     ticker := time.NewTicker(5 * time.Second)
//     defer ticker.Stop()

//     for {
//         select {
//         case <-ticker.C:
//             sendNotifyMessage(client)
//         }
//     }
// }

// // 处理客户端连接
// func handleConnection(conn net.Conn) {
//     client := &Client{
//         conn: conn,
//         authorized: false,
//         subscribed: false,
//         id: 0,
//     }
//     defer conn.Close()

//     buf := make([]byte, 4096)
//     for {
//         n, err := conn.Read(buf)
//         if err != nil {
//             log.Printf("Error reading from client: %v", err)
//             return
//         }

//         var msg StratumMessage
//         err = json.Unmarshal(buf[:n], &msg)
//         if err != nil {
//             log.Printf("Error unmarshaling JSON: %v", err)
//             sendErrorResponse(conn, msg.ID, -32700, "Parse error")
//             continue
//         }

//         log.Printf("Received message: %+v", msg)

//         switch msg.Method {
//         case "mining.subscribe":
//             handleSubscribe(client, msg.ID)
//             if client.subscribed && client.authorized {
//                 sendNotifyMessage(client)
//                 go sendPeriodicNotify(client)
//             }
//         case "mining.authorize":
//             handleAuthorize(client, msg.ID, msg.Params)
//             if client.subscribed && client.authorized {
//                 sendNotifyMessage(client)
//                 go sendPeriodicNotify(client)
//             }
//         case "mining.submit":
//             handleSubmit(client, msg.ID, msg.Params)
//         default:
//             sendErrorResponse(conn, msg.ID, -32601, "Method not found")
//         }
//     }
// }

// func sendSetTargetMessage(client *Client, target string) {
//     /*if!client.subscribed ||!client.authorized {
//         return
//     }*/

//     msg := StratumMessage{
//         ID: nil,
//         Method: "mining.set_target",
//         Params: []interface{}{target},
//     }

//     msgJSON, err := json.Marshal(msg)
//     if err != nil {
//         log.Printf("Error marshaling mining.set_target message: %v", err)
//         return
//     }

//     /*client.mu.Lock()
//     client.target = target
//     client.mu.Unlock()*/
//     msgJSON = append(msgJSON, '\n')
//     _, err = client.conn.Write(msgJSON)
//     if err != nil {
//         log.Printf("Error sending mining.set_target message: %v", err)
//     }
// }

// // 处理订阅消息
// func handleSubscribe(client *Client, id interface{}) {
//     client.mu.Lock()
//     client.subscribed = true
//     client.mu.Unlock()

//     // 模拟订阅响应
//     /*response := StratumResponse{
//         ID: id,
//         Result: []interface{}{
//             []string{"mining.notify", "ae6812eb4cd7735a302a8a9dd95cf71f"},
//             []string{"00000000000000000000000000000000", "ffffffffffffffffffffffffffffffff"},
//             4,
//         },
//         Error: nil,
//     }*/
//     response := StratumResponse{
//         ID: id,
//         Result: []interface{}{
//             nil, magicNum, 8,
//         },
//         Error: nil,
//     }

//     sendResponse(client.conn, response)

//     sendSetTargetMessage(client, testTarget)
// }

// // 处理授权消息
// func handleAuthorize(client *Client, id interface{}, params []interface{}) {
//     if len(params) < 2 {
//         sendErrorResponse(client.conn, id, -32602, "Invalid params")
//         return
//     }

//     username, ok := params[0].(string)
//     password, ok2 := params[1].(string)
//     if!ok ||!ok2 {
//         sendErrorResponse(client.conn, id, -32602, "Invalid params")
//         return
//     }

//     // 简单模拟授权逻辑
//     if username == "testuser" && password == "testuser" {
//         client.mu.Lock()
//         client.authorized = true
//         client.mu.Unlock()
//         sendResponse(client.conn, StratumResponse{
//             ID: id,
//             Result: true,
//             Error: nil,
//         })
//     } else {
//         sendResponse(client.conn, StratumResponse{
//             ID: id,
//             Result: false,
//             Error: nil,
//         })
//     }
// }

// // 处理提交消息
// func handleSubmit(client *Client, id interface{}, params []interface{}) {
//     if!client.subscribed ||!client.authorized {
//         sendErrorResponse(client.conn, id, -32000, "Not subscribed or authorized")
//         return
//     }

//     if len(params) < 3 {
//         sendErrorResponse(client.conn, id, -32602, "Invalid params")
//         return
//     }

//     // 简单模拟提交处理
//     log.Printf("Received share submission: %+v", params)
//     username, _ := params[0].(string)
//     jobid, _ := params[1].(string)
//     nonce, _ := params[2].(string)
//     fmt.Printf("username %s jobid %s nonce %s\n", username, jobid, nonce)

//     // new a BLAKE2s hash
//     hash, err := blake2sext.New256(nil)
//     if err != nil {
//         fmt.Println("Error creating Blake2s hash:", err)
//         sendErrorResponse(client.conn, id, -32001, "something error happen in server")
//         return
//     }

//     decimalUint, err := strconv.ParseUint(jobid, 10, 64)
//     if err != nil {
//         fmt.Printf("str to int failed %v\n", err)
//     } else {
//         fmt.Printf("str to int %08x\n", decimalUint)
//     }
//     //hexString := testTask
//     str := fmt.Sprintf("%016x", decimalUint)
//     hexString := str + testTask
//     //resultStr := "17cbeee16d478d739bbf3a41e6e0ef10aa5ad0d9d97b9e3cf98c470000000000"
//     hexString += nonce
//     hexString += magicNum
// 	fmt.Printf("Hexstring %v\n",hexString)
//     byteSlice, err := hex.DecodeString(hexString)
//     if err != nil {
//         //fmt.Printf("decode hex string error: %v\n", err)
//         sendErrorResponse(client.conn, id, -32002, "invalid format of nonce, not little endian 64 bit hex string")
//         return
//     }
//     _, err = hash.Write(byteSlice)
//     if err != nil {
//         //fmt.Println("Error writing to Blake2s hash:", err)
//         sendErrorResponse(client.conn, id, -32003, "hash error happen in server")
//         return
//     }
//     // compute hash
//     result := hash.Sum(nil)
//     // inverse the hash result
//     for i := 0; i < 16; i++ {
//         var temp = result[i]
//         result[i] = result[31-i]
//         result[31-i] = temp
//     }
//     var respRes = false
//     var respMsg = ""
//     // change the hash result to hex string
//     resultHex := hex.EncodeToString(result)
//     fmt.Printf("Blake2s hash input\n%s\nresult: %s\n", hexString, resultHex)
//     hashoutNum := new(big.Int)
//     // change the hex string to big.Int
//     _, success := hashoutNum.SetString(resultHex, 16)
//     if success {
//         targetNum := new(big.Int)
//         _, success2 := targetNum.SetString(testTarget, 16)
//         if success2 {
//             res := hashoutNum.Cmp(targetNum)
//             if res < 0 {
//                 //fmt.Println("compare success")
//                 respRes = true
//             } else {
//                 fmt.Println("compare failed")
//                 respMsg = "low difficulty share"
//             }
//         } else {
//             fmt.Println("change target to big number failed please check the input string")
//         }
//     } else {
//         fmt.Println("change hash result to big number failed please check the input string")
//     }

//     sendResponse(client.conn, StratumResponse{
//         ID: id,
//         Result: respRes,
//         Error: respMsg,
//     })
// }

// // 发送响应消息
// func sendResponse(conn net.Conn, response StratumResponse) {
//     log.Printf("sendResponse: %+v", response)
//     respJSON, err := json.Marshal(response)
//     if err != nil {
//         log.Printf("Error marshaling JSON response: %v", err)
//         return
//     }
//     respJSON = append(respJSON, '\n')
//     _, err = conn.Write(respJSON)
//     if err != nil {
//         log.Printf("Error writing response to client: %v", err)
//     }
// }

// // 发送错误响应消息
// func sendErrorResponse(conn net.Conn, id interface{}, code int, message string) {
//     response := StratumResponse{
//         ID: id,
//         Result: nil,
//         Error: []interface{}{code, message, nil},
//     }
//     sendResponse(conn, response)
// }

// func main() {
//     // 监听指定端口
//     listener, err := net.Listen("tcp", ":3333")
//     if err != nil {
//         log.Fatalf("Failed to listen: %v", err)
//     }
//     defer listener.Close()

//     fmt.Println("Stratum server is listening on port 3333...")

//     for {
//         // 接受客户端连接
//         conn, err := listener.Accept()
//         if err != nil {
//             log.Printf("Error accepting connection: %v", err)
//             continue
//         }

//         // 处理客户端连接
//         go handleConnection(conn)
//     }
// }