package logger

import (
	"strings"
	"testing"
)

func TestIPFSLogger_Buffering(t *testing.T) {
	l := NewIPFSLogger("http://localhost:5001")
	if l == nil {
		t.Fatal("Expected logger to be created")
	}

	l.Println("Test message 1")
	l.Printf("Test message %d", 2)

	content := l.buffer.String()
	if !strings.Contains(content, "Test message 1") {
		t.Errorf("Buffer should contain 'Test message 1', got: %s", content)
	}
	if !strings.Contains(content, "Test message 2") {
		t.Errorf("Buffer should contain 'Test message 2', got: %s", content)
	}
}

func TestIPFSLogger_Publish_NoNode(t *testing.T) {
	// This test attempts to publish to a likely non-existent node to verify error handling
	// or ensure it doesn't panic.
	l := NewIPFSLogger("http://localhost:12345") // Invalid port
	l.Println("Test log")

	_, err := l.Publish()
	if err == nil {
		t.Log("Warning: Publish succeeded unexpectedly (maybe something IS running there?)")
	} else {
		t.Logf("Publish correctly failed when node missing: %v", err)
	}
}
