package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"os"
	"strings"
	"time"

	shell "github.com/ipfs/go-ipfs-api"
	"github.com/joho/godotenv"
)

func main() {
	// 1. Load .env
	if err := godotenv.Load("../../.env"); err != nil {
		// Try loading from current dir just in case
		if err := godotenv.Load(); err != nil {
			log.Println("⚠️  Warning: Could not load .env file (checking environment variables directly)")
		} else {
			log.Println("✅ Loaded .env file")
		}
	} else {
		log.Println("✅ Loaded .env file (from ../../.env)")
	}

	// 2. Get Config
	url := os.Getenv("IPFS_NODE_URL")
	if url == "" {
		log.Fatal("❌ IPFS_NODE_URL is not set in environment or .env")
	}
	fmt.Printf("ℹ️  IPFS_NODE_URL: %s\n", url)

	// 3. Connect
	sh := shell.NewShell(url)

	// 4. Test Connectivity (Version)
	version, commit, err := sh.Version()
	if err != nil {
		log.Fatalf("❌ Failed to connect to IPFS node: %v\n(Is the daemon running?)", err)
	}
	fmt.Printf("✅ Connected to IPFS! (Version: %s, Commit: %s)\n", version, commit)

	// 5. Test Write
	content := fmt.Sprintf("IPFS Test from ASIC Pool Logger at %s", time.Now().Format(time.RFC3339))
	cid, err := sh.Add(strings.NewReader(content))
	if err != nil {
		log.Fatalf("❌ Failed to write to IPFS: %v", err)
	}

	fmt.Printf("✅ Write Successful! CID: %s\n", cid)

	// 6. Test MFS Write (Append)
	mfsPath := "/asic-pool-check.log"
	fmt.Printf("📝 Testing MFS Write to %s...\n", mfsPath)

	// Get current size for append
	var offset int64 = 0
	stat, err := sh.FilesStat(context.Background(), mfsPath)
	if err == nil {
		fmt.Printf("   File exists, current size: %d, CID: %s\n", stat.Size, stat.Hash)
		offset = int64(stat.Size)
	} else {
		fmt.Printf("   File does not exist yet (will be created)\n")
	}

	// Append
	mfsContent := fmt.Sprintf("[%s] MFS Check\n", time.Now().Format(time.RFC3339))

	// Create Multipart Body
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	// CreateFormFile with "data" field name (required by IPFS)
	fw, err := mw.CreateFormFile("data", "file")
	if err != nil {
		log.Fatalf("❌ Failed to create form file: %v", err)
	}
	if _, err := io.Copy(fw, strings.NewReader(mfsContent)); err != nil {
		log.Fatalf("❌ Failed to copy content: %v", err)
	}
	mw.Close()

	// Use generic Request with multipart body
	// api/v0/files/write?arg=<path>&create=true&parents=true&offset=<size>
	req := sh.Request("files/write", mfsPath)
	req.Option("create", true)
	req.Option("parents", true)
	req.Option("offset", offset)

	req.Body(&buf)
	req.Header("Content-Type", mw.FormDataContentType())

	if err := req.Exec(context.Background(), nil); err != nil {
		log.Fatalf("❌ MFS Write Failed: %v", err)
	}

	// Verify
	newStat, err := sh.FilesStat(context.Background(), mfsPath)
	if err != nil {
		log.Fatalf("❌ Failed to stat MFS file after write: %v", err)
	}
	fmt.Printf("✅ MFS Write Successful! New Size: %d, New CID: %s\n", newStat.Size, newStat.Hash)

	fmt.Println("🎉 IPFS Integration (Block + MFS) is working correctly.")
}
