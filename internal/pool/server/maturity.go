package server

import (
	"context"
	"log"
	"math/big"
	"time"

	"asicPool/internal/pool/storage"
	"asicPool/internal/pool/wallet"
)

// blockLifecycleManager runs in the background to check for block confirmations (Mature & Paid)
func blockLifecycleManager(db storage.Storage, walletClient wallet.Client, maturityDepth int, payoutDepth int) {
	log.Printf("Starting Block Lifecycle Manager (Maturity=%d, Payout=%d)", maturityDepth, payoutDepth)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if db == nil {
			continue
		}

		// 1. Get Current Chain Height
		tmpl, err := getBlockTemplate()
		if err != nil {
			log.Printf("Maturity: Failed to get chain tip: %v", err)
			continue
		}
		currentHeight := uint64(tmpl.Height)
		ctx := context.Background()

		// 2. Handle PENDING -> MATURE
		pending, err := db.GetPendingBlocks(ctx)
		if err == nil {
			for _, b := range pending {
				if currentHeight < b.Height {
					continue
				}
				confirmations := currentHeight - b.Height + 1
				if confirmations >= uint64(maturityDepth) {
					if err := db.UpdateBlockStatus(ctx, b.Hash, "MATURE"); err != nil {
						log.Printf("Maturity: Failed to update block %s: %v", b.Hash, err)
					} else {
						log.Printf("✅ Block %s MATURE (Confs %d)", b.Hash, confirmations)
					}
				}
			}
		}

		// 3. Handle MATURE -> PAID
		// Only proceed if wallet is available
		if walletClient == nil {
			continue
		}

		mature, err := db.GetMatureBlocks(ctx)
		if err == nil {
			for _, b := range mature {
				if currentHeight < b.Height {
					continue
				}
				confirmations := currentHeight - b.Height + 1
				if confirmations >= uint64(payoutDepth) {
					// Fetch credits for this block
					credits, err := db.GetUnpaidCredits(ctx, b.Hash)
					if err != nil {
						log.Printf("Payout: Failed to get credits for block %s: %v", b.Hash, err)
						continue
					}

					if len(credits) == 0 {
						// All paid or none exist, mark block as PAID
						if err := db.UpdateBlockStatus(ctx, b.Hash, "PAID"); err == nil {
							log.Printf("💰 Block %s fully PAID", b.Hash)
						}
						continue
					}

					log.Printf("💸 Processing Payouts for Block %s (%d credits)", b.Hash, len(credits))

					// 1. Proactive Balance Check
					totalReq := new(big.Int)
					for _, c := range credits {
						totalReq.Add(totalReq, new(big.Int).SetUint64(c.Amount))
					}

					balance, err := walletClient.GetBalance(ctx)
					if err != nil {
						log.Printf("Payout: Failed to get wallet balance: %v", err)
						continue
					}

					if balance.Cmp(totalReq) < 0 {
						log.Printf("⚠️ [Payout Paused] Insufficient Wallet Balance. Have %s wei, Need %s wei. Waiting...", balance.String(), totalReq.String())
						continue
					}

					allPaid := true
					for _, c := range credits {
						// Amount is already in Wei (stored by handler.go)
						amountWei := new(big.Int).SetUint64(c.Amount)
						// No extra scaling needed.

						txID, err := walletClient.SendTransaction(ctx, c.MinerAddress, amountWei)
						if err != nil {
							log.Printf("❌ Payout Failed for %s: %v", c.MinerAddress, err)
							allPaid = false
							// ABORT: Stop processing to let pending transactions clear
							log.Printf("⚠️ Payout loop aborted to prevent mempool overflow. Will retry in next tick.")
							break
						}

						log.Printf("  Sent %d to %s (Tx: %s)", c.Amount, c.MinerAddress, txID)
						db.MarkCreditPaid(ctx, c.ID, txID, amountWei, c.MinerAddress)
					}

					if !allPaid {
						// If we aborted or failed, stop processing subsequent blocks in this tick too
						break
					}

					if allPaid {
						// Mark as PAID
						if err := db.UpdateBlockStatus(ctx, b.Hash, "PAID"); err != nil {
							log.Printf("Payout: Failed to update block status %s: %v", b.Hash, err)
						} else {
							log.Printf("💰 Block %s marked as PAID", b.Hash)
						}
					}
				}
			}
		}
	}
}
