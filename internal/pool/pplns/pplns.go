package pplns

import (
	"sync"
	"time"
)

// PPLNS_SCALE is the multiplier to convert float64 difficulty to fixed-point uint64 work units.
// Using 1e9 as recommended to preserve precision.
const PPLNS_SCALE = 1_000_000_000

// Share represents a single valid share execution.
type Share struct {
	Account   string
	Work      uint64 // Difficulty * 1e9
	Timestamp int64  // Unix nano
}

// Window manages the rolling window of PPLNS shares.
type Window struct {
	mu            sync.RWMutex
	shares        []Share
	totalWork     uint64
	targetWork    uint64 // The 'N' in PPLNS (scaled)
	workByAccount map[string]uint64
}

// NewWindow creates a new PPLNS window with the specified target work (N).
// targetWork is the raw difficulty sum (e.g., 2000.0), which will be scaled internally.
func NewWindow(targetWork float64) *Window {
	return &Window{
		shares:        make([]Share, 0, 10000), // Pre-allocate some capacity
		targetWork:    uint64(targetWork * PPLNS_SCALE),
		workByAccount: make(map[string]uint64),
	}
}

// PushShare adds a new share to the window and evicts old ones if necessary.
// assignedPdiff should be the difficulty assigned to the miner (VarDiff).
func (w *Window) PushShare(account string, assignedPdiff float64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	work := uint64(assignedPdiff * PPLNS_SCALE)
	if work == 0 {
		return // Ignore 0 work
	}

	share := Share{
		Account:   account,
		Work:      work,
		Timestamp: time.Now().UnixNano(),
	}

	w.shares = append(w.shares, share)
	w.totalWork += work
	w.workByAccount[account] += work

	// Evict oldest shares if we exceed targetWork.
	// We strictly follow "last N work", so we pop until totalWork <= targetWork.
	//
	// NOTE: This implementation uses a "Strict Limit": TotalWork will never exceed TargetWork.
	// If removing the oldest share drops TotalWork below TargetWork, we still remove it.
	// This means the window size might fluctuate slightly below N_work temporarily.
	//
	// Alternative "Soft Limit" Approach:
	// Some implementations prefer to keep *at least* N work (Stop evicting if total < N).
	// We are currently using the Strict Limit to ensure we don't overpay for history.

	for len(w.shares) > 0 && w.totalWork > w.targetWork {
		// Since it's a slice, popping from front is O(N) memory move.
		// For a production pool with high throughput, a ring buffer would be more efficient.
		// For typical usage, the slice shift is acceptable.

		removed := w.shares[0]
		// Safety check: ensure we don't underflow
		if w.totalWork < removed.Work {
			w.totalWork = 0
		} else {
			// Proceed with eviction to maintain strict limit (TotalWork <= TargetWork)

			w.totalWork -= removed.Work
			w.workByAccount[removed.Account] -= removed.Work
			if w.workByAccount[removed.Account] == 0 {
				delete(w.workByAccount, removed.Account)
			}
			w.shares = w.shares[1:]
		}
	}
}

// Snapshot returns a copy of the current work distribution and total work.
// Useful for calculating rewards safely without holding the lock.
// Returns: map[account]workUnits, totalWorkUnits
func (w *Window) Snapshot() (map[string]uint64, uint64) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Deep copy map
	snapshot := make(map[string]uint64, len(w.workByAccount))
	for k, v := range w.workByAccount {
		snapshot[k] = v
	}

	return snapshot, w.totalWork
}

// CalculateCredits computes the reward for each account based on the snapshot.
// reward is the total block reward (in minimal units, e.g. satoshis/wei).
// Returns map[account]rewardAmount.
func CalculateCredits(workByAccount map[string]uint64, totalWork uint64, blockReward uint64) map[string]uint64 {
	credits := make(map[string]uint64)
	if totalWork == 0 {
		return credits
	}

	// Use big.Int for calculation to avoid overflow during multiply
	// credit = (accountWork * blockReward) / totalWork
	// We don't need fixed-point scaling here because work/totalWork is the fraction.

	// Note: integer division truncates. The remainder is dust.
	// In a real pool, dust is often collected or ignored.

	for acc, work := range workByAccount {
		if work == 0 {
			continue
		}
		// val = work * reward
		// val = val / totalWork

		// 128 bit math check:
		// work (u64) * reward (u64) can exceed u64.
		// Go doesn't have native u128.
		// We need to implement manual multiplication or split.
		// Since we didn't import math/big in signature, let's just use it inside.
		// Or simply assume uint64 is enough?
		// If reward is 50 BTC = 50 * 1e8 = 5,000,000,000.
		// If work is 1e9 * difficulty (say 1,000,000) = 1e15.
		// 5e9 * 1e15 = 5e24. Exceeds uint64 (1.8e19).
		// Use float64 or big.Int for credit calculation.
		// NOTE: totals can use uint64 with overflow checks or big.Int if needed.
		// We will use float64 for simplicity as PPLNS credit calc usually tolerates epsilon errors.
		// Let's use float64 for the calculation: credit = reward * (float(work)/float(total))

		frac := float64(work) / float64(totalWork)
		credit := uint64(float64(blockReward) * frac)
		credits[acc] = credit
	}
	return credits
}
