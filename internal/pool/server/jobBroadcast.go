package server

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	parentMu   sync.Mutex
	lastParent string

	currentTemplateKey atomic.Value
	lastBroadcastTime  atomic.Value

	// Store copy of the latest broadcast info so we can resend to individual clients
	lastSeq           atomic.Uint64
	lastBaseJobVal    atomic.Value // *JobData
	lastBaseNotifyVal atomic.Value // []interface{}
	currentParent     atomic.Value // holds string of current parent hash (normalized)

	// Rate limiting for getBlockTemplate
	lastGBTRequest     time.Time
	lastGBTRequestMu   sync.Mutex
	lastCachedTemplate *BlockTemplate

	// Pool Address for block template (fees/identification)
	poolAddress string
	// HTTP client with timeout to prevent blocking on slow/unresponsive nodes
	gbtHTTPClient = &http.Client{
		Timeout: 5 * time.Second,
	}

	// Freeze detection: track how long we've been on the same parent
	freezeDetectMu         sync.Mutex
	freezeDetectParent     string
	freezeDetectStart      time.Time
	freezeDetectLastWarn   time.Time
	freezeDetectWarnAfter  = 30 * time.Second // warn after 30s on same parent
	freezeDetectWarnRepeat = 60 * time.Second // repeat warning every 60s
)

const templateTTLDur = 10 * time.Second
const gbtMinInterval = 500 * time.Millisecond // Back pressure: min 500ms between GBT requests (reduced from 300ms to lower node load)

// Start both broadcasters from main():
//   go parentWatcher()
//   go jobBroadcaster()

// 1s hygiene broadcaster (keep-alive + VarDiff ticks)
func jobBroadcaster() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		pushVarDiffTicks()
	}
}

// Fast watcher: poll parent ~2x/sec and broadcast CLEAN as soon as it flips
func parentWatcher() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		tpl, err := getBlockTemplate()
		if err != nil {
			log.Printf("[GBT ERROR] getBlockTemplate failed: %v", err)
			continue
		}
		parent := normalizeHex(tpl.PreviousHash)

		parentMu.Lock()
		changed := (parent != lastParent)
		if changed {
			lastParent = parent
			currentParent.Store(parent)
		}
		parentMu.Unlock()

		// Freeze detection: warn if same parent for too long
		checkForFreeze(parent, changed, tpl)

		reason := "template update"
		force := false
		if changed {
			reason = fmt.Sprintf("tip change to %s", parent)

		} else if last, ok := lastBroadcastTime.Load().(time.Time); ok {
			age := time.Since(last)
			if age > templateTTLDur {
				force = true
				reason = fmt.Sprintf("TTL refresh (age=%s)", age.Truncate(time.Millisecond))
			}
		}
		maybeBroadcastTemplate(tpl, force, reason)
	}
}

// checkForFreeze detects when the blockchain appears frozen (same parent for too long)
func checkForFreeze(parent string, changed bool, tpl *BlockTemplate) {
	freezeDetectMu.Lock()
	defer freezeDetectMu.Unlock()

	now := time.Now()

	if changed || freezeDetectParent != parent {
		// Parent changed - reset tracking
		freezeDetectParent = parent
		freezeDetectStart = now
		freezeDetectLastWarn = time.Time{} // reset warning timer
		return
	}

	// Same parent - check how long
	staleDuration := now.Sub(freezeDetectStart)
	if staleDuration < freezeDetectWarnAfter {
		return // not stale enough to warn yet
	}

	// Check if we should emit a warning (first time or repeat interval)
	if freezeDetectLastWarn.IsZero() || now.Sub(freezeDetectLastWarn) >= freezeDetectWarnRepeat {
		freezeDetectLastWarn = now

		// Log detailed diagnostic info
		log.Printf("⚠️⚠️⚠️ [FREEZE DETECTED] Same parent hash for %.1f seconds!", staleDuration.Seconds())
		log.Printf("    Parent: %s", parent)
		log.Printf("    Height: %d", tpl.Height)
		log.Printf("    CurTime from template: %d (now: %d, age: %ds)",
			tpl.CurTime, now.Unix(), now.Unix()-int64(tpl.CurTime))
		log.Printf("    ⚠️ The blockchain node may be frozen or not accepting new blocks!")
		log.Printf("    ⚠️ Check node logs for errors, deadlocks, or sync issues.")
	}
}

func fetchAndBroadcastNewJob(reason string) {
	tpl, err := getBlockTemplate()
	if err != nil {
		log.Printf("template fetch error: %v", err)
		return
	}
	maybeBroadcastTemplate(tpl, true, reason)
}

func pushToAllMinersWithVarDiff(baseNotify []interface{}, baseJob *JobData, seq uint64) {
	// Store for single-client resends
	lastSeq.Store(seq)
	lastBaseJobVal.Store(baseJob)
	lastBaseNotifyVal.Store(baseNotify)

	clientsMu.Lock()
	baseJobID, _ := baseNotify[0].(string)

	now := time.Now()
	for c := range clients {
		pushJobToClient(c, baseNotify, baseJob, seq, baseJobID, false)

		// VarDiff periodic check (TIMER ONLY)
		// We do NOT call ResendJobToClient here recursively, because we just sent them a job!
		// But we might want to adjust if they've been silent.
		// NOTE: original code called adjustOnTimer here.
		if changed, newP, _ := c.adjustOnTimer(now); changed {
			if err := pushDifficultyUpdate(c, newP); err != nil {
				log.Printf("failed to push diff update: %v", err)
			}
			log.Printf("VarDiff timer update %s: pdiff=%.6f", c.addr, newP)
			// Since we just changed diff, we should technically resend the job again with new diff...
			// But we are inside the 'send to everyone' loop, so we can just send it correctly the first time?
			// Actually adjustOnTimer updates c.pdiff.
			// So if we call adjustOnTimer BEFORE pushJobToClient, we can send the correct one.
			// Let's optimize: check timer first.
		}
	}
	clientsMu.Unlock()
}

// Resend current job to a specific client (e.g. after difficulty change)
func ResendJobToClient(c *client) {
	baseNotify, _ := lastBaseNotifyVal.Load().([]interface{})
	baseJob, _ := lastBaseJobVal.Load().(*JobData)
	seq := lastSeq.Load()

	if baseNotify == nil || baseJob == nil {
		return // nothing to send
	}

	baseJobID, _ := baseNotify[0].(string)
	if err := pushJobToClient(c, baseNotify, baseJob, seq, baseJobID, true); err != nil {
		log.Printf("[ERROR]Error sending notify to %s: %v", c.addr, err)
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.conn.Close()
	}
}

func pushJobToClient(c *client, baseNotify []interface{}, baseJob *JobData, seq uint64, baseJobID string, clean bool) error {
	// 1) ensure extranonce1 exists
	c.mu.Lock()
	if c.extranonce1 == ([4]byte{}) {
		v := atomic.AddUint32(&en1Counter, 1)
		binary.LittleEndian.PutUint32(c.extranonce1[:], v)
		log.Printf("[DEBUG_NOTIFY] Extranonce1 was zero for %s, set to: %08x", c.addr, v)
	}
	en1Hex := hex.EncodeToString(c.extranonce1[:])
	c.mu.Unlock()

	if en1Hex == "00000000" {
		log.Printf("[ERROR] Extranonce1 is still zero for %s after initialization!", c.addr)
		return fmt.Errorf("extranonce1 is zero")
	}

	// 2) per-client jobID
	jobID := fmt.Sprintf("%s_%s", baseJobID, en1Hex)

	// 3) Get current difficulty
	c.diffMu.Lock()
	pdiff := c.pdiff
	prevPdiff := c.prevPdiff
	c.diffMu.Unlock()

	// Bootstrap if needed
	if pdiff <= 0 {
		pdiff = startingPdiff
	}

	// 4) Send difficulty FIRST
	// Note: We send this every time we send a job to ensure sync
	if err := pushDifficultyUpdate(c, pdiff); err != nil {
		return err
	}

	// 5) clone JobData with CORRECT difficulty values
	jobCopy := &JobData{
		Version:         baseJob.Version,
		TemplateSeq:     seq,
		NBits:           baseJob.NBits,
		ParentHash:      baseJob.ParentHash,
		NTime:           baseJob.NTime,
		CreatedAt:       baseJob.CreatedAt,
		TxRoot:          baseJob.TxRoot,
		StateRoot:       baseJob.StateRoot,
		Height:          baseJob.Height,
		CoinbaseAddress: baseJob.CoinbaseAddress,
		Reward:          baseJob.Reward,
		BlockFees:       baseJob.BlockFees,
		Difficulty:      pdiff,     // Store the EXACT pdiff we sent
		PrevDiff:        prevPdiff, // Store the EXACT prevPdiff
	}
	jobCopy.stale.Store(false)
	jobCopy.blockSubmitted.Store(false)

	// 6) build per-client notify
	notify := make([]interface{}, len(baseNotify))
	copy(notify, baseNotify)
	notify[0] = jobID

	// If this is a forced resend (clean=true), update the clean_jobs flag in field [5]
	// BlockDAG Params: [0:jobID, 1:prev, 2:ver, 3:bits, 4:time, 5:clean]
	if clean {
		notify[5] = true
	}

	// 7) store in acceptedJobs under per-client jobID
	acceptedJobs.Store(jobID, jobCopy)

	// 8) Send the job notification
	// log.Printf("[DEBUG_NOTIFY] Sending to %s: jobID=%s baseJobID=%s en1Hex=%s notify=%+v", c.addr, jobID, baseJobID, en1Hex, notify)
	if err := sendPush(c, "mining.notify", notify); err != nil {
		log.Printf("[ERROR]Error sending notify to %s: %v", c.addr, err)
		return err
	}

	// log.Printf("[DEBUG_NOTIFY] Successfully sent jobID=%s to %s (stored in acceptedJobs)", jobID, c.addr)
	return nil
}

func maybeBroadcastTemplate(tpl *BlockTemplate, force bool, reason string) {
	key := templateKey(tpl)
	prevKey, _ := currentTemplateKey.Load().(string)

	templateChanged := (key != prevKey)
	if !templateChanged && !force {
		return
	}

	seq := templateSeqCounter.Add(1)

	// If tip hash changed then pool should reject the work... for previous job
	// Implication: If tip hash is SAME, do NOT reject (do not mark stale).
	// Extract old parent from prevKey (format: parent|state|txroot|ver)
	prevParent := ""
	if parts := strings.Split(prevKey, "|"); len(parts) > 0 {
		prevParent = parts[0]
	}
	newParent := normalizeHex(tpl.PreviousHash)

	if newParent != prevParent {
		markAllJobsStale()
	}

	jobID := fmt.Sprintf("%d-%x", seq, time.Now().UnixNano())
	// Use templateChanged as the 'clean' flag. Periodic TTL refreshes (force=true but templateChanged=false)
	// will send clean=false, allowing miners to continue hashing without interruption.
	notify, job, err := buildNotify(jobID, tpl, templateChanged)
	if err != nil {
		log.Printf("failed to build notify for job %s: %v", jobID, err)
		return
	}
	job.TemplateSeq = seq

	currentTemplateKey.Store(key)
	lastBroadcastTime.Store(time.Now())

	if reason == "" {
		reason = "template refresh"
	}
	log.Printf("[REFRESH] %s (seq=%d parent=%s)", reason, seq, normalizeHex(tpl.PreviousHash))

	pushToAllMinersWithVarDiff(notify, job, seq)
}

func markAllJobsStale() {
	acceptedJobs.Range(func(key, value interface{}) bool {
		if job, ok := value.(*JobData); ok {
			job.stale.Store(true)
		}
		return true
	})
}

func templateKey(tpl *BlockTemplate) string {
	parent := normalizeHex(tpl.PreviousHash)
	state := normalizeHex(tpl.StateRoot)
	txRoot := normalizeHex(tpl.TxRoot)
	return fmt.Sprintf("%s|%s|%s|%d", parent, state, txRoot, tpl.Version)
}

func pushVarDiffTicks() {
	clientsMu.Lock()
	now := time.Now()
	for c := range clients {
		if changed, newP, _ := c.adjustOnTimer(now); changed {
			log.Printf("VarDiff timer update %s: pdiff=%.6f", c.addr, newP)
			ResendJobToClient(c) // This sends set_difficulty AND notify
		}
	}
	clientsMu.Unlock()
}

func forceTemplateRefresh(reason string) {
	// CRITICAL: Clear the cached template to force a fresh fetch from the node.
	// Without this, the rate limiter in getBlockTemplate() may return the old
	// cached template after a block submission, causing miners to work on stale jobs.
	lastGBTRequestMu.Lock()
	lastCachedTemplate = nil
	lastGBTRequestMu.Unlock()

	go fetchAndBroadcastNewJob(reason)
}

func jobReaper() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-5 * time.Minute)
		acceptedJobs.Range(func(key, value interface{}) bool {
			job, ok := value.(*JobData)
			if !ok {
				acceptedJobs.Delete(key)
				return true
			}
			if job.CreatedAt.Before(cutoff) {
				acceptedJobs.Delete(key)
			}
			return true
		})
	}
}

func getBlockTemplate() (*BlockTemplate, error) {
	// Rate limiting: enforce minimum interval between GBT requests
	lastGBTRequestMu.Lock()
	elapsed := time.Since(lastGBTRequest)
	if elapsed < gbtMinInterval && lastCachedTemplate != nil {
		// Return cached template if we're within the rate limit window
		cached := lastCachedTemplate
		lastGBTRequestMu.Unlock()
		return cached, nil
	}
	lastGBTRequest = time.Now()
	lastGBTRequestMu.Unlock()

	params := []interface{}{[]interface{}{}, 10}
	if poolAddress != "" {
		params = append(params, poolAddress)
	}

	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getBlockTemplate",
		"params":  params,
	}
	buf, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", rpcURL, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(rpcUser, rpcPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := gbtHTTPClient.Do(req)
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
		fmt.Printf("RPC Error in getBlockTemplate : %+v\n", rpcResp.Error)
		return nil, errors.New("RPC error in getBlockTemplate")
	}

	// Cache the template
	lastGBTRequestMu.Lock()
	lastCachedTemplate = &rpcResp.Result
	lastGBTRequestMu.Unlock()

	return &rpcResp.Result, nil
}

func buildNotify(jobID string, tmpl *BlockTemplate, clean bool) ([]interface{}, *JobData, error) {
	// Standard fields
	versionHex := fmt.Sprintf("%08x", uint32(tmpl.Version))
	ntimeHex := fmt.Sprintf("%08x", uint32(tmpl.CurTime))

	// Use the nbits from template as-is, but normalise formatting
	nbitsHex := strings.ToLower(strings.TrimPrefix(tmpl.PoWDiffReference.Nbits, "0x"))

	if len(nbitsHex) != 8 {
		// in case template is sloppy, normalise to 8 chars
		if v, err := strconv.ParseUint(nbitsHex, 16, 32); err == nil {
			nbitsHex = fmt.Sprintf("%08x", uint32(v))
		} else {
			return nil, nil, fmt.Errorf("invalid nbits: %v", err)
		}
	}

	nbitsVal, _ := strconv.ParseUint(nbitsHex, 16, 32)
	job := &JobData{
		Version:    uint32(tmpl.Version),
		NBits:      uint32(nbitsVal),
		ParentHash: normalizeHex(tmpl.PreviousHash),
		NTime:      uint32(tmpl.CurTime),
		CreatedAt:  time.Now(),
	}
	// Fill TxRoot.StateRoot / Height / Coinbase / Reward
	if txrootBytes, err := hex.DecodeString(strings.TrimPrefix(tmpl.TxRoot, "0x")); err == nil && len(txrootBytes) == 32 {
		job.TxRoot = txrootBytes
	}
	if staterootBytes, err := hex.DecodeString(strings.TrimPrefix(tmpl.StateRoot, "0x")); err == nil && len(staterootBytes) == 32 {
		job.StateRoot = staterootBytes
	}
	hLE := make([]byte, 8)
	binary.LittleEndian.PutUint64(hLE, uint64(tmpl.Height))
	job.Height = hLE

	if addrBytes, err := hex.DecodeString(strings.TrimPrefix(tmpl.CoinbaseAddress, "0x")); err == nil {
		job.CoinbaseAddress = addrBytes
	}

	rewLE := make([]byte, 8)
	binary.LittleEndian.PutUint64(rewLE, uint64(tmpl.Reward))
	job.Reward = rewLE

	feeLE := make([]byte, 8)
	binary.LittleEndian.PutUint64(feeLE, uint64(tmpl.BlockFees))
	job.BlockFees = feeLE

	// Normalize and PAD to 64 hex chars (32 bytes)
	prevLE := normalizeHex(tmpl.PreviousHash)
	if len(prevLE) < 64 {
		prevLE = strings.Repeat("0", 64-len(prevLE)) + prevLE
	}
	job.ParentHash = prevLE

	// BlockDAG Stratum: ["job_id","prevhash","bbversion","nbits","ntime",clean]
	// prevhash is hex string (32 bytes)
	// bbversion is hex string (4 bytes)
	// nbits is hex string (4 bytes)
	// ntime is hex string (4 bytes)

	notify := []interface{}{
		jobID,
		prevLE,     // prevhash (using same LE format as before, assuming spec implies this)
		versionHex, // bbversion
		nbitsHex,   // nbits
		ntimeHex,   // ntime
		clean,
	}
	return notify, job, nil
}

// CurrentParent returns the latest normalized parent hash (thread-safe)
func CurrentParent() string {
	s, _ := currentParent.Load().(string)
	return s
}
