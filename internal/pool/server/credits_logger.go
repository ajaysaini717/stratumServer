package server

import (
	"asicPool/internal/logger"
	"asicPool/internal/pool/pplns"
	"asicPool/internal/pool/storage"
	"context"
	"fmt"
	"log"
)

func logCredits(db storage.Storage, ipfsLog *logger.IPFSLogger, snapshot map[string]uint64, totalWork uint64, reward uint64, blockHash string, blockHeight uint64, maturityDepth int, payoutDepth int, poolFeePct float64) {
	// Calculate Pool Fee
	feeAmount := uint64(float64(reward) * (poolFeePct / 100.0))
	distributable := reward - feeAmount

	// Persist Block (if DB is connected)
	if db != nil {
		ctx := context.Background()
		if err := db.AddBlock(ctx, blockHash, blockHeight, reward, feeAmount); err != nil {
			log.Printf("❌ DB AddBlock Failed: %v", err)
		}
	}

	credits := pplns.CalculateCredits(snapshot, totalWork, distributable)

	// Persist Credits
	if db != nil {
		ctx := context.Background()
		if err := db.AddCredits(ctx, blockHash, credits); err != nil {
			log.Printf("❌ DB AddCredits Failed: %v", err)
		} else {
			log.Println("✅ Block & Credits saved to Database")
		}
	}

	// Logging Logic (StdOut + IPFS)
	logMsg := func(format string, v ...interface{}) {
		log.Printf(format, v...)
		if ipfsLog != nil {
			ipfsLog.Printf(format, v...)
		}
	}

	logMsg("💰 PPLNS SNAPSHOT for Block %s (Height %d): TotalWork=%d AccountCount=%d", blockHash, blockHeight, totalWork, len(snapshot))
	logMsg("💵 REVENUE: Total=%d PoolFee=%.2f%% (%d) Distributable=%d", reward, poolFeePct, feeAmount, distributable)
	logMsg("--- BLOCK CREDITS (PENDING) ---")
	logMsg("   (Credits mature at height %d, Payout unlock at height %d)", blockHeight+uint64(maturityDepth), blockHeight+uint64(payoutDepth))

	for acc, credit := range credits {
		logMsg("  Miner %s: %d (%.4f%%)", acc, credit, float64(credit)/float64(distributable)*100.0)
	}
	logMsg("-----------------------------")

	// Publish to IPFS
	if ipfsLog != nil {
		// 1. Publish to PubSub (Real-time stream)
		// We format a single string with all the relevant info or just a notification
		// For now, let's publish the full block credit summary we just logged.
		// (Simpler: just publish a "New Block Found" notification + the summary)
		// Since we logged multiple lines, let's reconstruct a brief summary for PubSub.
		pubSubMsg := fmt.Sprintf("BLOCK %s (Ht %d) | Reward: %d | Credits: %d miners", blockHash, blockHeight, reward, len(credits))
		if err := ipfsLog.PubSubPublish("asic-pool-logs", pubSubMsg); err != nil {
			log.Printf("⚠️ IPFS PubSub Failed: %v", err)
		} else {
			// log.Printf("📡 Published to IPFS PubSub topic 'asic-pool-logs'")
		}

		// 2. Append to MFS (Archival)
		newCID, err := ipfsLog.Publish()
		if err != nil {
			log.Printf("⚠️ IPFS MFS Append Failed: %v", err)
		} else {
			log.Printf("💾 Log appended to /asic-pool-credits.log | New Root CID: %s", newCID)
		}
	}
}
