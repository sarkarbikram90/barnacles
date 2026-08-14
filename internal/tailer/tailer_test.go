package tailer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTailerNormalAppend(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "app.log")

	// Create initial file
	if err := os.WriteFile(logPath, []byte("line 1\nline 2\n"), 0o600); err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tail, err := New(ctx, Config{
		Path:          logPath,
		StartPosition: "beginning",
		PollInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create tailer: %v", err)
	}
	defer tail.Close()

	// Read initial lines
	assertNextLine(t, tail.Lines(), "line 1")
	assertNextLine(t, tail.Lines(), "line 2")

	// Append more lines
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("failed to open for append: %v", err)
	}
	if _, err := f.WriteString("line 3\nline 4\n"); err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	_ = f.Close()

	assertNextLine(t, tail.Lines(), "line 3")
	assertNextLine(t, tail.Lines(), "line 4")
}

func TestTailerStartPositionEnd(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "app.log")

	// Create pre-existing file with lines
	if err := os.WriteFile(logPath, []byte("old line 1\nold line 2\n"), 0o600); err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tail, err := New(ctx, Config{
		Path:          logPath,
		StartPosition: "end",
		PollInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create tailer: %v", err)
	}
	defer tail.Close()

	// Append a new line after tailer started
	time.Sleep(50 * time.Millisecond)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("failed to open for append: %v", err)
	}
	if _, err := f.WriteString("new line after start\n"); err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	_ = f.Close()

	// Should only receive the new line
	assertNextLine(t, tail.Lines(), "new line after start")
}

func TestTailerPartialLines(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "partial.log")

	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tail, err := New(ctx, Config{
		Path:          logPath,
		StartPosition: "beginning",
		PollInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create tailer: %v", err)
	}
	defer tail.Close()

	// Write partial line
	_, _ = f.WriteString("hel")
	_ = f.Sync()

	// Ensure nothing received yet
	select {
	case line := <-tail.Lines():
		t.Fatalf("unexpected line received before newline: %s", line)
	case <-time.After(50 * time.Millisecond):
	}

	// Complete the line
	_, _ = f.WriteString("lo world\n")
	_ = f.Sync()
	_ = f.Close()

	assertNextLine(t, tail.Lines(), "hello world")
}

func TestTailerRotation(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "rotation.log")
	rotPath := filepath.Join(tempDir, "rotation.log.1")

	// Create initial file
	if err := os.WriteFile(logPath, []byte("rot line 1\n"), 0o600); err != nil {
		t.Fatalf("failed to write initial file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tail, err := New(ctx, Config{
		Path:          logPath,
		StartPosition: "beginning",
		PollInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create tailer: %v", err)
	}
	defer tail.Close()

	assertNextLine(t, tail.Lines(), "rot line 1")

	// Rotate file: rename rotation.log -> rotation.log.1
	if err := os.Rename(logPath, rotPath); err != nil {
		t.Fatalf("failed to rename for rotation: %v", err)
	}

	// Create new rotation.log
	if err := os.WriteFile(logPath, []byte("rot line 2 in new file\n"), 0o600); err != nil {
		t.Fatalf("failed to write new file: %v", err)
	}

	assertNextLine(t, tail.Lines(), "rot line 2 in new file")
}

func TestTailerTruncation(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "trunc.log")

	// Create file with long content
	if err := os.WriteFile(logPath, []byte("line long 1\nline long 2\n"), 0o600); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tail, err := New(ctx, Config{
		Path:          logPath,
		StartPosition: "beginning",
		PollInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create tailer: %v", err)
	}
	defer tail.Close()

	assertNextLine(t, tail.Lines(), "line long 1")
	assertNextLine(t, tail.Lines(), "line long 2")

	// Truncate file and write fresh line
	if err := os.WriteFile(logPath, []byte("fresh after trunc\n"), 0o600); err != nil {
		t.Fatalf("failed to truncate file: %v", err)
	}

	assertNextLine(t, tail.Lines(), "fresh after trunc")
}

func assertNextLine(t *testing.T, ch <-chan string, expected string) {
	t.Helper()
	select {
	case line, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed unexpectedly while waiting for %q", expected)
		}
		if line != expected {
			t.Fatalf("expected line %q, got %q", expected, line)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for line %q", expected)
	}
}
