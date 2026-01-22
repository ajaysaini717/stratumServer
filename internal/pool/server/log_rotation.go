package server

import (
	"asicPool/internal/logger"
	"asicPool/internal/pool/storage"
	"context"
	"log"
	"time"
)

// StartLogRotationManager runs a background goroutine that flushes IPFS logs every interval
// and persists the resulting CID in the database.
func StartLogRotationManager(db storage.Storage, ipfsLog *logger.IPFSLogger, interval time.Duration) {
	if ipfsLog == nil {
		log.Println("LogRotationManager: IPFS logger not available, skipping.")
		return
	}

	log.Printf("Starting Log Rotation Manager (Interval: %v)", interval)

	// We use a ticker to trigger every interval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		performLogRotation(db, ipfsLog)
	}
}

func performLogRotation(db storage.Storage, ipfsLog *logger.IPFSLogger) {
	log.Println("🔄 Performing scheduled log rotation...")

	newCID, err := ipfsLog.Publish()
	if err != nil {
		log.Printf("❌ Log Rotation Failed: IPFS Publish error: %v", err)
		return
	}

	if newCID == "" {
		log.Println("ℹ️ Log Rotation: No logs to publish in this interval.")
		return
	}

	log.Printf("💾 Log Rotation Successful | New CID: %s", newCID)
	log.Println("✅ Log Rotation Completed.")

	if db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := db.AddLogRotation(ctx, newCID); err != nil {
			log.Printf("❌ Log Rotation: Failed to save CID to DB: %v", err)
		} else {
			log.Println("✅ Log Rotation CID saved to Database")
		}
	}
}
