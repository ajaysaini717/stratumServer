package logger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"strings"
	"time"

	shell "github.com/ipfs/go-ipfs-api"
)

// IPFSLogger handles logging to an IPFS node.
type IPFSLogger struct {
	sh     *shell.Shell
	buffer *bytes.Buffer
}

// NewIPFSLogger creates a new IPFS logger connected to the specified node URL.
func NewIPFSLogger(url string) *IPFSLogger {
	if url == "" {
		return nil
	}
	sh := shell.NewShell(url)
	return &IPFSLogger{
		sh:     sh,
		buffer: new(bytes.Buffer),
	}
}

// Printf formats according to a format specifier and writes to the internal buffer.
func (l *IPFSLogger) Printf(format string, v ...interface{}) {
	if l == nil {
		return
	}
	// Add timestamp to each log line
	msg := fmt.Sprintf(format, v...)
	timestamp := time.Now().Format(time.RFC3339)
	l.buffer.WriteString(fmt.Sprintf("[%s] %s\n", timestamp, msg))
}

// Println formats using the default formats for its operands and writes to the internal buffer.
func (l *IPFSLogger) Println(v ...interface{}) {
	if l == nil {
		return
	}
	msg := fmt.Sprint(v...)
	timestamp := time.Now().Format(time.RFC3339)
	l.buffer.WriteString(fmt.Sprintf("[%s] %s\n", timestamp, msg))
}

// Publish appends the buffered logs to a single file on IPFS MFS (/asic-pool-credits.log)
// and returns the new CID of that file.
func (l *IPFSLogger) Publish() (string, error) {
	if l == nil || l.sh == nil {
		return "", fmt.Errorf("IPFS logger not initialized")
	}

	if l.buffer.Len() == 0 {
		return "", nil // Nothing to publish
	}

	// 1. Prepare to append to MFS file
	// Path: /asic-pool-credits.log
	filePath := "/asic-pool-credits.log"

	content := l.buffer.String()
	reader := strings.NewReader(content)

	// Determine offset for append
	var offset int64 = 0
	stat0, err := l.sh.FilesStat(context.Background(), filePath)
	if err == nil {
		offset = int64(stat0.Size)
	}

	// Args: path, data, options
	// We use "files/write" with 'offset' to append, as there is no direct 'append' flag in HTTP API.

	// Prepare Multipart Body
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	// CreateFormFile with "data" property (required)
	fw, err := mw.CreateFormFile("data", "file")
	if err != nil {
		return "", fmt.Errorf("failed to create multipart form: %w", err)
	}
	if _, err := io.Copy(fw, reader); err != nil {
		return "", fmt.Errorf("failed to copy content to multipart: %w", err)
	}
	mw.Close()

	req := l.sh.Request("files/write", filePath)
	req.Option("create", true)
	req.Option("parents", true)
	req.Option("offset", offset)

	// Send body
	req.Body(&buf)
	req.Header("Content-Type", mw.FormDataContentType())

	if err := req.Exec(context.Background(), nil); err != nil {
		return "", fmt.Errorf("failed to append to IPFS file: %w", err)
	}

	// 2. Get the new CID of the updated file
	stat, err := l.sh.FilesStat(context.Background(), filePath)
	if err != nil {
		return "", fmt.Errorf("failed to stat IPFS file: %w", err)
	}

	// Clear buffer only on success
	l.buffer.Reset()

	return stat.Hash, nil
}

// PubSubPublish publishes a message to a specific IPFS PubSub topic.
// This allows subscribers (ipfs pubsub sub <topic>) to see logs in real-time.
func (l *IPFSLogger) PubSubPublish(topic string, message string) error {
	if l == nil || l.sh == nil {
		return fmt.Errorf("IPFS logger not initialized")
	}
	return l.sh.PubSubPublish(topic, message)
}
