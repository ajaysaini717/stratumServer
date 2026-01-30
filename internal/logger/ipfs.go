package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	shell "github.com/ipfs/go-ipfs-api"
)

// IPFSLogger handles logging to an IPFS node with local file buffering.
type IPFSLogger struct {
	sh              *shell.Shell
	logDir          string
	currentFile     *os.File
	currentFileName string
	mu              sync.Mutex
}

// NewIPFSLogger creates a new IPFS logger. It ensures the log directory exists
// and opens the initial log file.
func NewIPFSLogger(url string, logDir string) *IPFSLogger {
	if url == "" {
		return nil
	}
	if logDir == "" {
		logDir = "logs"
	}

	// Ensure log directory exists
	if err := os.MkdirAll(logDir, 0755); err != nil {
		fmt.Printf("⚠️ Failed to create log directory %s: %v\n", logDir, err)
		return nil
	}

	sh := shell.NewShell(url)
	l := &IPFSLogger{
		sh:     sh,
		logDir: logDir,
	}

	// Open initial file
	if err := l.rotateFile(); err != nil {
		fmt.Printf("⚠️ Failed to initialize log file: %v\n", err)
		return nil
	}

	return l
}

func (l *IPFSLogger) rotateFile() error {
	timestamp := time.Now().In(getISTLocation()).Format("20060102-150405")
	fileName := filepath.Join(l.logDir, fmt.Sprintf("pool-%s.txt", timestamp))

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file %s: %w", fileName, err)
	}

	if l.currentFile != nil {
		l.currentFile.Close()
	}

	l.currentFile = file
	l.currentFileName = fileName
	return nil
}

var (
	istLocation *time.Location
	istOnce     sync.Once
)

func getISTLocation() *time.Location {
	istOnce.Do(func() {
		var err error
		istLocation, err = time.LoadLocation("Asia/Kolkata")
		if err != nil {
			// Fallback to fixed offset if zoneinfo is missing
			istLocation = time.FixedZone("IST", 5.5*60*60)
		}
	})
	return istLocation
}

// Printf formats according to a format specifier and writes to the current log file.
func (l *IPFSLogger) Printf(format string, v ...interface{}) {
	if l == nil || l.currentFile == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	// Add timestamp to each log line in IST
	msg := fmt.Sprintf(format, v...)
	timestamp := time.Now().In(getISTLocation()).Format(time.RFC3339)
	line := fmt.Sprintf("[%s] %s\n", timestamp, msg)
	_, _ = l.currentFile.WriteString(line)
}

// Println formats using the default formats and writes to the current log file.
func (l *IPFSLogger) Println(v ...interface{}) {
	if l == nil || l.currentFile == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprint(v...)
	timestamp := time.Now().In(getISTLocation()).Format(time.RFC3339)
	line := fmt.Sprintf("[%s] %s\n", timestamp, msg)
	_, _ = l.currentFile.WriteString(line)
}

// Publish uploads the current log file to IPFS, closes it, and starts a new one.
// It returns the CID of the uploaded file.
func (l *IPFSLogger) Publish() (string, error) {
	if l == nil || l.sh == nil {
		return "", fmt.Errorf("IPFS logger not initialized")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Sync and close current file
	if l.currentFile != nil {
		// Check if file is empty before publishing
		info, err := l.currentFile.Stat()
		if err == nil && info.Size() == 0 {
			return "", nil // Nothing to publish
		}
		l.currentFile.Close()
	}

	// 2. Open the file for reading to upload to IPFS
	fileToUpload, err := os.Open(l.currentFileName)
	if err != nil {
		// Try to recover by opening a new file
		_ = l.rotateFile()
		return "", fmt.Errorf("failed to open file for upload: %w", err)
	}
	defer fileToUpload.Close()

	// 3. Add to IPFS
	cid, err := l.sh.Add(fileToUpload)
	if err != nil {
		// Try to recover current file handle
		l.currentFile, _ = os.OpenFile(l.currentFileName, os.O_APPEND|os.O_WRONLY, 0644)
		return "", fmt.Errorf("failed to add logs to IPFS: %w", err)
	}

	// 4. Rotate to a new file
	if err := l.rotateFile(); err != nil {
		return cid, fmt.Errorf("IPFS upload succeeded (CID: %s) but failed to start new log file: %w", cid, err)
	}

	return cid, nil
}

// PubSubPublish publishes a message to a specific IPFS PubSub topic.
func (l *IPFSLogger) PubSubPublish(topic string, message string) error {
	if l == nil || l.sh == nil {
		return fmt.Errorf("IPFS logger not initialized")
	}
	return l.sh.PubSubPublish(topic, message)
}
