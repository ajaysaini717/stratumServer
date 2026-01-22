package server

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log"
	"math/big"
	"strings"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
)

// ------------------- Utilities -------------------

var Diff1TargetPool = func() *big.Int {
	n := new(big.Int)
	// EXACT same diff1 as node-stratum-pool / BTC
	n.SetString("00000000ffff0000000000000000000000000000000000000000000000000000", 16)
	return n
}()

const ShareMultiplier = 65536.0 // scrypt share multiplier (2^16)

// HashToBig converts a common.Hash into a big.Int that can be used to
// perform math comparisons.
func HashToBig(hash *common.Hash) *big.Int {
	// A Hash is in little-endian, but the big package wants the bytes in
	// big-endian, so reverse them.
	buf := *hash
	blen := len(buf)
	for i := 0; i < blen/2; i++ {
		buf[i], buf[blen-1-i] = buf[blen-1-i], buf[i]
	}

	return new(big.Int).SetBytes(buf[:])
}

func bitsToTarget(compact uint32) *big.Int {
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

	// Make it negative if the sign bit is set.
	if isNegative {
		bn = bn.Neg(bn)
	}

	return bn
}

func sendResult(c *client, id interface{}, result interface{}) {
	resp := stratumResponse{ID: id, Result: result, Error: nil}
	sendJSON(c, resp)
}

func sendError(c *client, id interface{}, code int, message string) {
	resp := stratumResponse{ID: id, Result: nil, Error: []interface{}{code, message}}
	sendJSON(c, resp)
}

func sendPush(c *client, method string, params interface{}) error {
	notify := map[string]interface{}{
		"id":     nil,
		"method": method,
		"params": params,
	}
	return sendJSON(c, notify)
}

func sendJSON(c *client, v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("[%s] marshal error: %v", c.addr, err)
		return err
	}

	if _, err := c.rw.Write(b); err != nil {
		log.Printf("[%s] write error: %v", c.addr, err)
		// Already holding c.mu from the defer above - don't lock again!
		c.closed = true
		c.conn.Close()
		return err
	}
	if _, err := c.rw.WriteString("\n"); err != nil {
		log.Printf("[%s] write error: %v", c.addr, err)
		// Already holding c.mu from the defer above - don't lock again!
		c.closed = true
		c.conn.Close()
		return err
	}
	if err := c.rw.Flush(); err != nil {
		log.Printf("[%s] flush error: %v", c.addr, err)
		// Already holding c.mu from the defer above - don't lock again!
		c.closed = true
		c.conn.Close()
		return err
	}
	return nil
}

func rawParamsToStrings(raw []json.RawMessage) []string {
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		var s string
		if err := json.Unmarshal(r, &s); err == nil {
			out = append(out, s)
			continue
		}
		var n json.Number
		if err := json.Unmarshal(r, &n); err == nil {
			out = append(out, n.String())
			continue
		}
		out = append(out, strings.Trim(string(r), `"`))
	}
	return out
}

func normalizeHex(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "0x")
	return strings.ToLower(s)
}

// dummyMerkleRoot implements the "pure dummy" merkle scheme:
// coinb1 = "", coinb2 = "", merkle_branch = []
// so merkle_root = dsha256(extranonce1 || extranonce2).
func dummyMerkleRoot(extranonce1, extranonce2 []byte) [32]byte {
	buf := make([]byte, 0, len(extranonce1)+len(extranonce2))
	buf = append(buf, extranonce1...)
	buf = append(buf, extranonce2...)

	h1 := sha256.Sum256(buf)
	h2 := sha256.Sum256(h1[:])
	return h2
}

// sendSetExtranonce generates a new extranonce1 and sends mining.set_extranonce
func sendSetExtranonce(c *client) {
	c.mu.Lock()
	subscribed := c.extranonceSubscribed
	c.mu.Unlock()

	if !subscribed {
		return // Client didn't ask for updates
	}

	// Generate new unique extranonce1
	v := atomic.AddUint32(&en1Counter, 1)
	newEn1Bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(newEn1Bytes, v)

	c.mu.Lock()
	c.extranonce1 = [4]byte{}
	copy(c.extranonce1[:], newEn1Bytes)
	c.mu.Unlock()

	newEn1Hex := hex.EncodeToString(newEn1Bytes)
	en2Size := subscribeExtranonce2Size

	// Send notification: {"method": "mining.set_extranonce", "params": ["<en1>", <en2_size>]}
	sendPush(c, "mining.set_extranonce", []interface{}{newEn1Hex, en2Size})
	log.Printf("[%s] sent mining.set_extranonce en1=%s", c.addr, newEn1Hex)
}
