package logger

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	shell "github.com/ipfs/go-ipfs-api"
)

// IPFSLogger handles logging to an IPFS node.
type IPFSLogger struct {
	sh     *shell.Shell
	buffer *bytes.Buffer
	mu     sync.Mutex
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

// Printf formats according to a format specifier and writes to the internal buffer.
func (l *IPFSLogger) Printf(format string, v ...interface{}) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	// Add timestamp to each log line in IST
	msg := fmt.Sprintf(format, v...)
	timestamp := time.Now().In(getISTLocation()).Format(time.RFC3339)
	l.buffer.WriteString(fmt.Sprintf("[%s] %s\n", timestamp, msg))
}

// Println formats using the default formats for its operands and writes to the internal buffer.
func (l *IPFSLogger) Println(v ...interface{}) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprint(v...)
	timestamp := time.Now().In(getISTLocation()).Format(time.RFC3339)
	l.buffer.WriteString(fmt.Sprintf("[%s] %s\n", timestamp, msg))
}

// Publish adds the buffered logs to IPFS and returns the new CID.
// Each call returns a unique CID for the current buffer contents,
// and the buffer is cleared on success.
func (l *IPFSLogger) Publish() (string, error) {
	if l == nil || l.sh == nil {
		return "", fmt.Errorf("IPFS logger not initialized")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.buffer.Len() == 0 {
		return "", nil // Nothing to publish
	}

	content := l.buffer.String()
	reader := strings.NewReader(content)

	// Add the content to IPFS. This returns the CID of the unique blob.
	cid, err := l.sh.Add(reader)
	if err != nil {
		return "", fmt.Errorf("failed to add logs to IPFS: %w", err)
	}

	// Clear buffer only on success
	l.buffer.Reset()

	return cid, nil
}

// PubSubPublish publishes a message to a specific IPFS PubSub topic.
// This allows subscribers (ipfs pubsub sub <topic>) to see logs in real-time.
func (l *IPFSLogger) PubSubPublish(topic string, message string) error {
	if l == nil || l.sh == nil {
		return fmt.Errorf("IPFS logger not initialized")
	}
	return l.sh.PubSubPublish(topic, message)
}
