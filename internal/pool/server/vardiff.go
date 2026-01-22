package server

import (
	"log"
	"math"
	"time"
)

const (
	windowSize              = 60 * time.Second // Slower adjustment for stability
	desiredSharesPerWindow  = 20.0             // Expect more shares before adjusting
	toleranceFraction       = 0.5              // 50% tolerance
	noShareEasierFactor     = 0.3              // Drop 70% if no shares
	stepExponent            = 0.2              // Very slow adjustments
	minPdiff                = 0.0001      // Allow extremly low diff (for testnet)
	maxPdiff                = 10000.0          // Allow difficulty to rise high for ASICs
	startingPdiff           = 0.01       // Target ~2^244 (Easier than 2^242 Block). Prevents "Every Share is a Block".
	minFastRetargetInterval = 10 * time.Second // Allow fast adjustment (e.g. for ASICs joining)
)

// Called on each accepted share
// - increments window counter
// - if the window has elapsed, performs an adjustment immediately
func (c *client) recordShareAndAdjust(now time.Time) (changed bool, pdiff float64, avgInterval time.Duration) {
	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	// bootstrap
	if c.pdiff <= 0 {
		c.pdiff = startingPdiff
	}
	if c.windowStart.IsZero() {
		c.windowStart = now
	}

	c.windowShares++
	pdiff = c.pdiff

	// early exit if window not complete yet
	winDur := now.Sub(c.windowStart)
	if winDur < windowSize {
		// FAST RETARGET: If we have WAY too many shares already, don't wait for the full window.
		// e.g. if we have > 2x desired shares, adjust NOW.
		// BUT: Respect a cooldown to let the miner catch up (avoid rejecting valid shares).
		if float64(c.windowShares) > desiredSharesPerWindow*2 {
			if now.Sub(c.lastAdjust) >= minFastRetargetInterval {
				// Trigger adjustment immediately
				return c.adjustLocked(now, winDur)
			}
		}

		// Provide an interval hint for logs/dashboards
		if c.windowShares > 0 {
			avgInterval = time.Duration(float64(winDur) / float64(c.windowShares))
		}
		return false, pdiff, avgInterval
	}

	// Window completed → adjust
	changed, newP, avg := c.adjustLocked(now, winDur)
	return changed, newP, avg
}

// Called periodically even if NO shares arrived in the window.
// Use this from your broadcaster/timer loop.
func (c *client) adjustOnTimer(now time.Time) (changed bool, newPdiff float64, avgInterval time.Duration) {
	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	// Bootstrap: start a fresh window
	if c.windowStart.IsZero() {
		c.windowStart = now
		return false, c.pdiff, 0
	}

	winDur := now.Sub(c.windowStart)
	if winDur < windowSize {
		return false, c.pdiff, 0
	}
	return c.adjustLocked(now, winDur)
}

// Core adjust logic (expects diffMu held)
func (c *client) adjustLocked(now time.Time, winDur time.Duration) (changed bool, newPdiff float64, avgInterval time.Duration) {
	old := c.pdiff
	shares := float64(c.windowShares)

	// Compute ratio against target
	var ratio float64
	if shares <= 0 {
		// No shares: ease difficulty aggressively
		newPdiff = clamp(old*noShareEasierFactor, minPdiff, maxPdiff)
		ratio = 0.0
		avgInterval = 0
	} else {
		// Calculate expected shares for the actual window duration
		expectedShares := desiredSharesPerWindow * (winDur.Seconds() / windowSize.Seconds())
		ratio = shares / expectedShares
		// Deadband to avoid tiny oscillations
		lower := 1.0 - toleranceFraction
		upper := 1.0 + toleranceFraction

		if ratio < lower {
			// Too few shares → make easier
			scale := math.Pow(ratio, stepExponent) // < 1.0
			newPdiff = clamp(old*scale, minPdiff, maxPdiff)
		} else if ratio > upper {
			// Too many shares → make harder
			scale := math.Pow(ratio, stepExponent) // > 1.0
			newPdiff = clamp(old*scale, minPdiff, maxPdiff)
		} else {
			// Within deadband → keep pdiff
			newPdiff = old
		}

		avgInterval = time.Duration(float64(winDur) / shares)
	}

	// Reset window
	c.windowShares = 0
	c.lastAdjust = now
	c.windowStart = now

	if almostEqual(newPdiff, old) {
		log.Printf("[vardiff DEBUG] hold pdiff %.6f (shares=%.0f in %ds, ratio=%.2f, lower=%.2f upper=%.2f)",
			old, shares, int(winDur.Seconds()), ratio, 1.0-toleranceFraction, 1.0+toleranceFraction)
		return false, old, avgInterval
	}

	// JS-style semantics:
	// - prevPdiff holds the old difficulty
	// - pdiff becomes the new difficulty
	c.prevPdiff = old
	c.pdiff = newPdiff

	dir := "hold"
	if newPdiff > old {
		dir = "increase"
	} else if newPdiff < old {
		dir = "decrease"
	}
	log.Printf("[vardiff] %s %s pdiff %.6f → %.6f (shares=%.0f in %ds, target=%.0f/%ds)",
		c.addr, dir, old, newPdiff, shares, int(winDur.Seconds()),
		desiredSharesPerWindow, int(windowSize.Seconds()))
	return true, newPdiff, avgInterval
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func almostEqual(a, b float64) bool {
	if a == b {
		return true
	}
	// relative epsilon
	den := math.Max(math.Abs(a), math.Abs(b))
	if den == 0 {
		return false
	}
	return math.Abs(a-b)/den < 1e-3
}
