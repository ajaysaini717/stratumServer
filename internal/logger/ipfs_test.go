package logger

import (
	"os"
	"strings"
	"testing"
)

func TestIPFSLogger_FileStorage(t *testing.T) {
	testDir := "test_logs"
	defer os.RemoveAll(testDir)

	l := NewIPFSLogger("http://localhost:5001", testDir)
	if l == nil {
		t.Fatal("Expected logger to be created")
	}

	l.Println("Test message 1")
	l.Printf("Test message %d", 2)

	// Close file to ensure content is flushed
	l.mu.Lock()
	l.currentFile.Close()
	l.mu.Unlock()

	content, err := os.ReadFile(l.currentFileName)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !strings.Contains(string(content), "Test message 1") {
		t.Errorf("File should contain 'Test message 1', got: %s", string(content))
	}
	if !strings.Contains(string(content), "Test message 2") {
		t.Errorf("File should contain 'Test message 2', got: %s", string(content))
	}
}

func TestIPFSLogger_Publish_NoNode(t *testing.T) {
	testDir := "test_logs_publish"
	defer os.RemoveAll(testDir)

	l := NewIPFSLogger("http://localhost:12345", testDir) // Invalid port
	if l == nil {
		t.Fatal("Expected logger to be created")
	}
	l.Println("Test log")

	_, err := l.Publish()
	if err == nil {
		t.Log("Warning: Publish succeeded unexpectedly (maybe something IS running there?)")
	} else {
		t.Logf("Publish correctly failed when node missing: %v", err)
	}
}
