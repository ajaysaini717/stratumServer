package pplns

import (
	"testing"
)

func TestPPLNS_Fairness(t *testing.T) {
	// N_WORK = 1000 * SCALE
	// Miner A: diff 16, 10 shares => 160 work
	// Miner B: diff 1, 160 shares => 160 work
	// Total Work: 320
	// Both should have equal work recorded.

	target := 1000.0
	window := NewWindow(target)

	// Miner A
	for i := 0; i < 10; i++ {
		window.PushShare("minerA", 16.0)
	}

	// Miner B
	for i := 0; i < 160; i++ {
		window.PushShare("minerB", 1.0)
	}

	snap, total := window.Snapshot()

	expectedA := uint64(160 * PPLNS_SCALE)
	expectedB := uint64(160 * PPLNS_SCALE)
	expectedTotal := expectedA + expectedB

	if total != expectedTotal {
		t.Errorf("Expected total %d, got %d", expectedTotal, total)
	}

	if snap["minerA"] != expectedA {
		t.Errorf("MinerA work mismatch: got %d, want %d", snap["minerA"], expectedA)
	}

	if snap["minerB"] != expectedB {
		t.Errorf("MinerB work mismatch: got %d, want %d", snap["minerB"], expectedB)
	}
}

func TestPPLNS_Eviction(t *testing.T) {
	// Target = 100 units
	target := 100.0
	window := NewWindow(target)

	// Fill with 10 shares of 10 diff (Total 100)
	for i := 0; i < 10; i++ {
		window.PushShare("minerA", 10.0)
	}

	// Verify full
	_, total := window.Snapshot()
	if total != uint64(100*PPLNS_SCALE) {
		t.Fatalf("Setup failed: total %d", total)
	}

	// Push 1 share of 10 diff for minerB
	// This should evict 1 share of minerA (oldest)
	// New total should be (100 - 10) + 10 = 100
	window.PushShare("minerB", 10.0)

	snap, total2 := window.Snapshot()
	if total2 > uint64(100*PPLNS_SCALE) {
		t.Errorf("Total work exceeded target after push: %d", total2)
	}

	// MinerA should have 90 work (9 shares)
	// MinerB should have 10 work (1 share)
	expectedA := uint64(90 * PPLNS_SCALE)
	expectedB := uint64(10 * PPLNS_SCALE)

	if snap["minerA"] != expectedA {
		t.Errorf("MinerA work incorrect: got %d, want %d", snap["minerA"], expectedA)
	}
	if snap["minerB"] != expectedB {
		t.Errorf("MinerB work incorrect: got %d, want %d", snap["minerB"], expectedB)
	}
}

func TestPPLNS_RampUp(t *testing.T) {
	// Target huge, push small amount
	window := NewWindow(1000.0)
	window.PushShare("minerA", 10.0)

	snap, total := window.Snapshot()
	if total != uint64(10*PPLNS_SCALE) {
		t.Errorf("Total during rampup wrong: %d", total)
	}
	if snap["minerA"] != uint64(10*PPLNS_SCALE) {
		t.Errorf("MinerA work wrong")
	}
}

func TestCalculateCredits(t *testing.T) {
	work := map[string]uint64{
		"A": 50,
		"B": 50,
	}
	total := uint64(100)
	reward := uint64(1000)

	credits := CalculateCredits(work, total, reward)

	if credits["A"] != 500 {
		t.Errorf("A credit wrong: %d", credits["A"])
	}
	if credits["B"] != 500 {
		t.Errorf("B credit wrong: %d", credits["B"])
	}
}
