package tailer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckpointRegistrySaveLoad(t *testing.T) {
	tempDir := t.TempDir()
	regPath := filepath.Join(tempDir, "checkpoints.json")

	// Phase 1: create registry, record checkpoints and save
	reg1, err := NewCheckpointRegistry(regPath, 0)
	if err != nil {
		t.Fatalf("NewCheckpointRegistry failed: %v", err)
	}

	cp1 := Checkpoint{
		DeviceID:          1001,
		FileIdentity:      50001,
		FilePath:          "/var/log/app.log",
		ByteOffset:        12345,
		LastSeenTimestamp: time.Now().UTC(),
	}
	cp2 := Checkpoint{
		DeviceID:          1001,
		FileIdentity:      50002,
		FilePath:          "/var/log/access.log",
		ByteOffset:        67890,
		LastSeenTimestamp: time.Now().UTC(),
	}

	reg1.Set(cp1)
	reg1.Set(cp2)

	if err := reg1.Save(); err != nil {
		t.Fatalf("reg1.Save() failed: %v", err)
	}
	if err := reg1.Close(); err != nil {
		t.Fatalf("reg1.Close() failed: %v", err)
	}

	// Phase 2: reload registry from disk and verify entries
	reg2, err := NewCheckpointRegistry(regPath, 0)
	if err != nil {
		t.Fatalf("NewCheckpointRegistry load failed: %v", err)
	}
	defer reg2.Close()

	if reg2.Count() != 2 {
		t.Fatalf("expected 2 checkpoints, got %d", reg2.Count())
	}

	loaded1, ok1 := reg2.Get(1001, 50001)
	if !ok1 || loaded1.ByteOffset != 12345 || loaded1.FilePath != "/var/log/app.log" {
		t.Fatalf("unexpected loaded checkpoint 1: %+v", loaded1)
	}

	loaded2, ok2 := reg2.Get(1001, 50002)
	if !ok2 || loaded2.ByteOffset != 67890 || loaded2.FilePath != "/var/log/access.log" {
		t.Fatalf("unexpected loaded checkpoint 2: %+v", loaded2)
	}
}

func TestTailerCheckpointResumption(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "resumption.log")
	regPath := filepath.Join(tempDir, "checkpoints.json")

	// Write initial lines
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	_, _ = f.WriteString("line 1\nline 2\n")
	_ = f.Sync()

	reg1, err := NewCheckpointRegistry(regPath, 0)
	if err != nil {
		t.Fatalf("NewCheckpointRegistry failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tailer1, err := New(ctx, Config{
		Path:               logPath,
		StartPosition:      "beginning",
		PollInterval:       10 * time.Millisecond,
		CheckpointRegistry: reg1,
	})
	if err != nil {
		t.Fatalf("New tailer failed: %v", err)
	}

	// Read lines 1 & 2
	line1 := <-tailer1.Lines()
	line2 := <-tailer1.Lines()
	if line1 != "line 1" || line2 != "line 2" {
		t.Fatalf("unexpected lines: %s, %s", line1, line2)
	}

	// Close tailer1 and registry1, ensuring checkpoints are flushed to disk
	_ = tailer1.Close()
	_ = reg1.Close()

	// Append line 3 while tailer is offline
	_, _ = f.WriteString("line 3\n")
	_ = f.Sync()
	_ = f.Close()

	// Restart tailer with new registry instance pointing to same checkpoint file
	reg2, err := NewCheckpointRegistry(regPath, 0)
	if err != nil {
		t.Fatalf("NewCheckpointRegistry reload failed: %v", err)
	}
	defer reg2.Close()

	tailer2, err := New(ctx, Config{
		Path:               logPath,
		StartPosition:      "beginning", // Even if config says "beginning", checkpoint must override!
		PollInterval:       10 * time.Millisecond,
		CheckpointRegistry: reg2,
	})
	if err != nil {
		t.Fatalf("New tailer2 failed: %v", err)
	}
	defer tailer2.Close()

	// Should receive ONLY line 3 (not line 1 or line 2)
	select {
	case line3 := <-tailer2.Lines():
		if line3 != "line 3" {
			t.Fatalf("expected 'line 3', got %q", line3)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for line 3 after resumption")
	}

	// Ensure no duplicate lines emitted
	select {
	case extra := <-tailer2.Lines():
		t.Fatalf("unexpected extra line received: %q", extra)
	case <-time.After(100 * time.Millisecond):
		// Success: no duplicates
	}
}
