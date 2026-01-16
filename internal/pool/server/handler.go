// pool_handlers.go
package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// -------------------- Handlers --------------------
// TTL for a job locally (if older than jobTTL, reject locally)
const jobTTL = 60 * time.Second

func handleSubscribe(c *client, req *stratumRequest) {
	c.mu.Lock()
	v := atomic.AddUint32(&en1Counter, 1)
	binary.LittleEndian.PutUint32(c.extranonce1[:], v)

	extranonce1 := hex.EncodeToString(c.extranonce1[:])
	c.mu.Unlock()
	extranonce2Size := subscribeExtranonce2Size // standard extranonce2 size

	// Build dynamic subscriptions (Stratum)
	// Spec: [[["mining.set_difficulty","sessionid"],["mining.notify","sessionid"]],"extra_nonce1",len]

	// Generate unique random Session ID (16 bytes = 32 hex chars)
	sidBytes := make([]byte, 16)
	_, _ = rand.Read(sidBytes)
	sessionID := hex.EncodeToString(sidBytes)
	c.sessionID = sessionID

	subscriptions := [][]interface{}{
		{"mining.set_difficulty", sessionID},
		{"mining.notify", sessionID},
	}

	// Prepare response
	res := []interface{}{
		subscriptions,
		extranonce1,
		extranonce2Size,
	}
	// Send response to miner
	sendResult(c, req.ID, res)
}

func validateUser(username string) (bool, string) {
	if username == "" {
		return false, "empty username"
	}

	// Strict: Username MUST be just the address
	address := username

	// EVM Address Validation: ^0x[0-9a-fA-F]{40}$
	// Length must be 42 (2 prefix + 40 hex)
	if len(address) != 42 || !strings.HasPrefix(address, "0x") {
		return false, "username should be a valid evm address (start with 0x...)"
	}

	// Validate hex characters
	validHex := regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
	if !validHex.MatchString(address) {
		return false, "username should be a valid evm address"
	}

	return true, ""
}

func handleAuthorize(c *client, req *stratumRequest) {
	fmt.Println("handleAuthorize")
	params := rawParamsToStrings(req.Params)
	if len(params) < 1 {
		sendError(c, req.ID, 24, "missing parameters")
		return
	}
	username := params[0] // logic: user.worker

	ok, errMsg := validateUser(username)
	if !ok {
		// Send the helpful error message to the miner
		sendError(c, req.ID, 24, errMsg)
		log.Printf("❌ [%s] authorize failed (bad user: %s) -- %s", c.addr, username, errMsg)
		return
	}

	c.username = username
	c.authorized = true

	// Track Miner in DB
	if c.storage != nil {
		go func() {
			if err := c.storage.AddMiner(context.Background(), username); err != nil {
				log.Printf("⚠️ Failed to track miner %s: %v", username, err)
			}
		}()
	}

	log.Printf("Miner authorized: %s", username)
	sendResult(c, req.ID, true)
	log.Printf("✅ [%s] authorize accepted user=%s", c.addr, username)
}

func handleExtranonceSubscribe(c *client, req *stratumRequest) {
	c.mu.Lock()
	c.extranonceSubscribed = true
	c.mu.Unlock()

	sendResult(c, req.ID, true)
	log.Printf("[%s] extranonce.subscribe accepted (state=true)", c.addr)
}

// ------------------- Submit Handler (uses stored template) -------------------

func handleSubmit(c *client, req *stratumRequest) error {
	params := rawParamsToStrings(req.Params)
	if len(params) < 5 {
		sendError(c, req.ID, -32602, "invalid params")
		return nil
	}

	worker, jobid, extranonce2, ntime, nonce := params[0], params[1], params[2], params[3], params[4]
	log.Printf("submit from worker=%s job=%s extranonce2=%s ntime=%s nonce=%s", worker, jobid, extranonce2, ntime, nonce)

	tplVal, ok := acceptedJobs.Load(jobid)
	if !ok {
		sendError(c, req.ID, 21, "stale job")
		// Soft error: don't disconnect
		return nil
	}
	jobData, ok := tplVal.(*JobData)
	if !ok || jobData == nil {
		sendError(c, req.ID, -32602, "invalid stored template")
		return nil
	}

	ttlExpired := false
	if jobTTL > 0 && time.Since(jobData.CreatedAt) > jobTTL {
		ttlExpired = true
		forceTemplateRefresh("local TTL expired")
	}

	// Decode extranonce2
	en2Bytes, err := hex.DecodeString(extranonce2)
	if err != nil || len(en2Bytes) != 4 {
		sendError(c, req.ID, -32602, "invalid extranonce2")
		return nil
	}

	header := make([]byte, 80)

	// BlockDAG Spec Header Construction (80 bytes)
	// input = bbversion + prevhash + sha256(sha256(extra_nonce1 + extra_nonce2)) + ntime + nbits + nonce
	// ALL FIELDS must match the hex string order (Big Endian equivalent)

	// 1. Version (4 bytes)
	binary.BigEndian.PutUint32(header[0:4], jobData.Version)

	// 2. PrevHash (32 bytes)
	prevBytes, _ := hex.DecodeString(jobData.ParentHash)
	copy(header[4:36], prevBytes)

	// 3. Merkle Root (32 bytes) = dsha256(en1 + en2)
	merkleRoot := dummyMerkleRoot(c.extranonce1[:], en2Bytes)
	copy(header[36:68], merkleRoot[:]) // Direct copy, NO reversal
	// 4. nTime (4 bytes)
	ntimeBytes, err := hex.DecodeString(ntime)
	if err != nil || len(ntimeBytes) != 4 {
		sendError(c, req.ID, 20, "invalid ntime")
		return nil
	}
	// Direct copy (miner sends BE string matching input)
	copy(header[68:72], ntimeBytes)
	// 5. nBits (4 bytes)
	binary.BigEndian.PutUint32(header[72:76], jobData.NBits)

	// 6. Nonce (4 bytes)
	nonceBytes, err := hex.DecodeString(nonce)
	if err != nil || len(nonceBytes) != 4 {
		sendError(c, req.ID, 20, "invalid nonce")
		return nil
	}
	// Direct copy
	copy(header[76:80], nonceBytes)
	result := scryptHash(header)
	hashInt := HashToBig(&result)
	networkTarget := bitsToTarget(jobData.NBits)
	assignedPdiff := jobData.Difficulty
	prevDiff := jobData.PrevDiff

	if assignedPdiff <= 0 {
		sendError(c, req.ID, 23, "server error: invalid assigned difficulty")
		return nil
	}

	shareDiffBF := new(big.Float).Quo(
		new(big.Float).Mul(new(big.Float).SetInt(Diff1TargetPool), big.NewFloat(ShareMultiplier)),
		new(big.Float).SetInt(hashInt),
	)
	shareDiff, _ := shareDiffBF.Float64()

	// Parse extranonce2 for RPC (don’t hardcode 0)
	var en2 uint64
	if extranonce2 != "" {
		en2, err = strconv.ParseUint(strings.TrimPrefix(extranonce2, "0x"), 16, 64)
		if err != nil {
			sendError(c, req.ID, -32602, "invalid extranonce2")
			// Soft error
			return nil
		}
	}

	// ----------------------------------------------------------------
	// Duplicate Share Check (Replay Protection)
	// Key = nonce + extranonce2 + ntime (sufficient uniqueness per job)
	// ----------------------------------------------------------------
	dupKey := fmt.Sprintf("%s-%s-%s", nonce, extranonce2, ntime)
	if _, loaded := jobData.usedNonces.LoadOrStore(dupKey, true); loaded {
		log.Printf("⚠️ REJECT duplicate share worker=%s job=%s key=%s", worker, jobid, dupKey)
		sendError(c, req.ID, 22, "duplicate share")
		return nil
	}

	// Only reject if the job's parent hash doesn't match the current network tip.
	// We ignore sequence lag (template updates on same tip) to minimize stale rejects.
	currentTip := CurrentParent()
	jobStale := jobData.stale.Load()
	if currentTip != "" && jobData.ParentHash != currentTip {
		jobStale = true
	}

	// If the only reason for staleness is local TTL expiry, still allow block submissions.
	if jobStale && ttlExpired && hashInt.Cmp(networkTarget) <= 0 {
		jobStale = false
	}

	// ----------------------------------------------------------------
	// Stale job check: never turn stale work into blocks.
	// ----------------------------------------------------------------
	if jobStale {
		if hashInt.Cmp(networkTarget) <= 0 {
			log.Printf("[STALE JOB] Block candidate FOUND in stale job - attempting submission anyway! worker=%s job=%s hash=%x", worker, jobid, result[:])
			// Do NOT return; fall through to block submission logic
		} else {
			sendError(c, req.ID, 21, "stale job")
			return nil
		}
	}

	// --------------------------------------------------------------------
	// 1) VALIDATE SHARE DIFFICULTY FIRST (Before Block Check)
	//    or check if it's a block. Ideally a block is also a valid share.
	// --------------------------------------------------------------------

	// Add 1% tolerance for rounding/precision issues
	threshold := assignedPdiff * 0.99
	isLowDiff := false

	if shareDiff < threshold {
		// Also apply tolerance to previous difficulty check
		prevThreshold := prevDiff * 0.99
		if prevDiff > 0 && shareDiff >= prevThreshold {
			// Treat as valid at previous difficulty (VarDiff transition safety)
			log.Printf("ℹ️ share meets previous diff but not new diff; "+
				"worker=%s job=%s shareDiff≈%.6f prevDiff=%.6f pdiff=%.6f",
				worker, jobid, shareDiff, prevDiff, assignedPdiff)
			assignedPdiff = prevDiff
		} else {
			isLowDiff = true
		}
	}

	// Variables to capture recordShareAndAdjust results
	var diffChanged bool
	var newPdiff float64

	// ----------------------------------------------------------------
	// 2) PUSH TO PPLNS (If valid share)
	// ----------------------------------------------------------------
	// We do this BEFORE the block check return, so the block share itself counts!
	if !isLowDiff {
		now := time.Now()
		// Update VarDiff stats
		diffChanged, newPdiff, _ = c.recordShareAndAdjust(now)

		// Push to PPLNS Window
		if PPLNSWindow != nil {
			PPLNSWindow.PushShare(c.username, assignedPdiff)
		}
	}

	// ----------------------------------------------------------------
	// 3) LOW DIFFICULTY CHECK
	// ----------------------------------------------------------------
	if isLowDiff {
		// STRICT: If it didn't meet pool target, we usually reject it.
		// BUT: What if it's a BLOCK?
		// Stratum spec: If hash <= networkTarget, it is a valid block, even if shareDiff < pdiff.
		// (Though practically pdiff is usually << networkTarget, so this is rare/impossible unless diff=1).
		// We will allow block submission even if "low share diff",
		// but we WON'T count it as a PPLNS share if it's below pdiff (to prevent spam).

		if hashInt.Cmp(networkTarget) > 0 {
			// Not a block, and low diff -> Reject
			errPayload := []interface{}{23, "low difficulty share", hex.EncodeToString(header), hex.EncodeToString(result[:])}
			resp := stratumResponse{ID: req.ID, Result: nil, Error: errPayload}
			sendJSON(c, resp)
			log.Printf("❌ low difficulty share worker=%s job=%s shareDiff≈%.6f pdiff=%.6f",
				worker, jobid, shareDiff, assignedPdiff)
			return nil
		}
	}

	// ----------------------------------------------------------------
	// 4) BLOCK CHECK
	// ----------------------------------------------------------------
	if hashInt.Cmp(networkTarget) <= 0 {
		if !jobData.blockSubmitted.CompareAndSwap(false, true) {
			log.Printf("[DUP BLOCK] duplicate block submission worker=%s job=%s hash=%x", worker, jobid, result[:])
			sendError(c, req.ID, 22, "duplicate block share")
			return nil
		}
		jobData.stale.Store(true)
		log.Printf("🎯 BLOCK FOUND height(le)=%d job=%s hash=%x target=%x",
			binary.LittleEndian.Uint64(jobData.Height), jobid, result[:], networkTarget.Bytes())

		// Build pow9 and submit ...
		// (Shortened for brevity, keeping original logic structure)
		// Re-using the logic from original code:

		pow9 := make([]byte, 9)
		pow9[0] = 0x0a
		copy(pow9[5:9], header[76:80]) // nonce

		full := make([]byte, 144+9+32)
		binary.LittleEndian.PutUint32(full[0:4], jobData.Version)
		copy(full[4:36], header[4:36])
		copy(full[36:68], jobData.TxRoot)
		copy(full[68:100], jobData.StateRoot)
		copy(full[100:132], header[36:68])
		binary.LittleEndian.PutUint32(full[132:136], jobData.NBits)
		ntVal := binary.BigEndian.Uint32(header[68:72])
		binary.LittleEndian.PutUint32(full[136:140], ntVal)
		copy(full[140:148], jobData.Height)
		copy(full[148:168], jobData.CoinbaseAddress)
		copy(full[168:176], jobData.Reward)
		copy(full[176:185], pow9)

		checkHash := scryptHash(header)
		checkInt := HashToBig(&checkHash)
		if checkInt.Cmp(networkTarget) > 0 {
			log.Printf("DROP: local recheck check=%x > target=%x", checkHash[:], networkTarget.Bytes())
			sendResult(c, req.ID, []interface{}{true, hex.EncodeToString(header), hex.EncodeToString(result[:])})
			return nil
		}

		finalHex := hex.EncodeToString(full)
		res, rpcErr := submitBlockHeader(finalHex, en2)
		if rpcErr != nil {
			msg := rpcErr.Error()
			if strings.Contains(msg, "overdue") || strings.Contains(msg, "expired") {
				log.Printf("⚠️ Block submission too late: %v", msg)
			} else {
				log.Printf("❌ submitBlockHeader error: %v", rpcErr)
			}
			forceTemplateRefresh("block submit error")
		} else {
			log.Printf("✅ Block submitted successfully: %+v", res)
			forceTemplateRefresh("block submission processed")

			// PPLNS: Snapshot (Share should already be in window now!)
			if PPLNSWindow != nil {
				snapshot, totalWork := PPLNSWindow.Snapshot()
				log.Printf("💰 PPLNS SNAPSHOT for Block %x: TotalWork=%d AccountCount=%d", result[:], totalWork, len(snapshot))
				if len(jobData.Reward) >= 8 {
					rewardVal := binary.LittleEndian.Uint64(jobData.Reward)

					// Add EVM Fees if available
					var feeVal uint64
					if len(jobData.BlockFees) >= 8 {
						feeVal = binary.LittleEndian.Uint64(jobData.BlockFees)
					}
					// FIX: Unit Conversion
					// Reward is in Atomic Units (10^8).
					// BlockFees is in Wei (10^18 base).
					// We need to convert Reward to Wei (10^18).
					// Factor = 10^18 / 10^8 = 10^10.

					// 1 Atomic Unit = 10,000,000,000 Wei
					rewardWei := rewardVal * 10_000_000_000
					totalReward := rewardWei + feeVal

					log.Printf("💰 Block Reward: Subsidy=%d Atomic (%d Wei), Fees=%d Wei, Total=%d Wei",
						rewardVal, rewardWei, feeVal, totalReward)

					// Pass Configured Maturity Depth
					// jobData.Height is []byte (little endian). We need to convert it.
					heightVal := binary.LittleEndian.Uint64(jobData.Height)
					logCredits(c.storage, c.ipfsLogger, snapshot, totalWork, totalReward, fmt.Sprintf("%x", result), heightVal, c.cfg.BlockMaturityDepth, c.cfg.PayoutMaturityDepth, c.cfg.PoolFeePercentage)
				}
			}
		}

		sendResult(c, req.ID, []interface{}{true, hex.EncodeToString(header), hex.EncodeToString(result[:])})
		return nil
	}

	// --------------------------------------------------------------------
	// 5) VALID SHARE RESPONSE (If not a block)
	// --------------------------------------------------------------------
	// We already recorded the share above.

	log.Printf("✅ valid share accepted %.6f → %.0f  worker=%s  job=%s",
		shareDiff, shareDiff*65536, worker, jobid)

	// Debug: return header and hash
	sendResult(c, req.ID, []interface{}{true, hex.EncodeToString(header), hex.EncodeToString(result[:])})

	// Push new difficulty immediately if it changed
	// (Note: we updated diff inside the 'if !isLowDiff' block via recordShareAndAdjust, but didn't capture return values.
	//  We need those return values! Refactoring to capture them.)
	if diffChanged {
		log.Printf("[vardiff] %s → pdiff %.8f", c.addr, newPdiff)
		ResendJobToClient(c)
	}
	return nil
}

// ShareTargetFromDiff computes the share target for a given pool difficulty.
//
// For diff >= 1: target = powLimit / diff
// For diff < 1:  target > powLimit (easier shares), which is OK for Stratum.
func ShareTargetFromDiff(Diff1TargetPool *big.Int, diff float64) *big.Int {
	fLimit := new(big.Float).SetInt(Diff1TargetPool)
	fDiff := big.NewFloat(diff)
	fT := new(big.Float).Quo(fLimit, fDiff)

	t := new(big.Int)
	fT.Int(t) // floor
	return t
}

func TargetToLittleEndianHex(target *big.Int) string {
	if target == nil {
		return strings.Repeat("00", 32)
	}
	if target.Sign() < 0 {
		target = new(big.Int)
	}
	bytes := target.Bytes()
	if len(bytes) > 32 {
		bytes = bytes[len(bytes)-32:]
	}
	buf := make([]byte, 32)
	copy(buf[32-len(bytes):], bytes)
	for i := 0; i < len(buf)/2; i++ {
		buf[i], buf[len(buf)-1-i] = buf[len(buf)-1-i], buf[i]
	}
	return hex.EncodeToString(buf)
}

func pushDifficultyUpdate(c *client, pdiff float64) error {
	log.Printf("PUSHDIF -> %s mining.set_difficulty %.8f", c.addr, pdiff)
	if err := sendPush(c, "mining.set_difficulty", []interface{}{pdiff}); err != nil {
		log.Printf("[ERROR]Error setting difficulty %v", err)
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return err
	}
	return nil
}

// targetToCompact packs a full target (*big.Int) into Bitcoin-style compact uint32
func targetToCompact(target *big.Int) uint32 {
	if target.Sign() == 0 {
		return 0
	}
	tb := target.Bytes() // big-endian bytes
	size := len(tb)
	var compact uint32
	if size <= 3 {
		var mantissa uint32
		for i := 0; i < size; i++ {
			mantissa <<= 8
			mantissa |= uint32(tb[i])
		}
		mantissa <<= 8 * (3 - size)
		compact = uint32(mantissa) | uint32(size<<24)
	} else {
		mantissa := uint32(tb[0])<<16 | uint32(tb[1])<<8 | uint32(tb[2])
		compact = uint32(mantissa) | uint32(size<<24)
		if (mantissa & 0x00800000) != 0 {
			mantissa >>= 8
			size++
			compact = uint32(mantissa) | uint32(size<<24)
		}
	}
	return compact
}

// TargetToCompactHex returns 8-char hex string of compact bits (nBits)
func TargetToCompactHex(target *big.Int) string {
	bits := targetToCompact(target)
	return fmt.Sprintf("%08x", bits)
}

func GetPowLimit() *big.Int {
	// For SHARE difficulty math we want the diff baseline,
	// not your chain's consensus powLimit.
	return Diff1TargetPool
}
