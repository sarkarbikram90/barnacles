package spool

import (
	"errors"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

func TestSpoolPushPop(t *testing.T) {
	tempDir := t.TempDir()
	s, err := New(tempDir, 10)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer s.Close()

	events1 := []logentry.LogEntry{
		logentry.New("h1", "app", "INFO", "msg 1", nil),
		logentry.New("h1", "app", "INFO", "msg 2", nil),
	}
	events2 := []logentry.LogEntry{
		logentry.New("h1", "app", "ERROR", "msg 3", nil),
	}

	if err := s.Push(events1); err != nil {
		t.Fatalf("Push(events1) failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond) // Ensure unique timestamp ordering
	if err := s.Push(events2); err != nil {
		t.Fatalf("Push(events2) failed: %v", err)
	}

	if count := s.Count(); count != 2 {
		t.Fatalf("expected 2 files, got %d", count)
	}
	bytesUsed, totalEvents := s.Size()
	if bytesUsed <= 0 || totalEvents != 3 {
		t.Fatalf("unexpected size/events: bytes=%d, events=%d", bytesUsed, totalEvents)
	}

	// Pop 1
	popped1, err := s.Pop()
	if err != nil {
		t.Fatalf("Pop() failed: %v", err)
	}
	if len(popped1) != 2 || popped1[0].Message != "msg 1" || popped1[1].Message != "msg 2" {
		t.Fatalf("unexpected batch 1: %+v", popped1)
	}

	// Pop 2
	popped2, err := s.Pop()
	if err != nil {
		t.Fatalf("Pop() failed: %v", err)
	}
	if len(popped2) != 1 || popped2[0].Message != "msg 3" {
		t.Fatalf("unexpected batch 2: %+v", popped2)
	}

	// Pop 3 (empty)
	_, err = s.Pop()
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}
}

func TestSpoolRecoveryOnRestart(t *testing.T) {
	tempDir := t.TempDir()

	// Stage 1: write items and close
	s1, err := New(tempDir, 10)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	events := []logentry.LogEntry{
		logentry.New("h1", "app", "WARN", "recoverable event", nil),
	}
	if err := s1.Push(events); err != nil {
		t.Fatalf("Push() failed: %v", err)
	}
	_ = s1.Close()

	// Stage 2: simulate restart with new instance on same directory
	s2, err := New(tempDir, 10)
	if err != nil {
		t.Fatalf("New() after restart failed: %v", err)
	}
	defer s2.Close()

	if count := s2.Count(); count != 1 {
		t.Fatalf("expected 1 recovered file, got %d", count)
	}

	popped, err := s2.Pop()
	if err != nil {
		t.Fatalf("Pop() after restart failed: %v", err)
	}
	if len(popped) != 1 || popped[0].Message != "recoverable event" {
		t.Fatalf("unexpected recovered event: %+v", popped)
	}
}

func TestSpoolCapacityLimit(t *testing.T) {
	tempDir := t.TempDir()
	// Set very small max size (e.g. 1MB)
	s, err := New(tempDir, 1)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer s.Close()

	// Manually set maxSizeBytes to fit exactly 2 batches (~580 bytes) so 3rd (~870 bytes) triggers pruning
	s.maxSizeBytes = 650 // bytes

	// Push 3 batches
	batchA := []logentry.LogEntry{logentry.New("h1", "s1", "INFO", "batch A message 1234567890", nil)}
	batchB := []logentry.LogEntry{logentry.New("h1", "s1", "INFO", "batch B message 1234567890", nil)}
	batchC := []logentry.LogEntry{logentry.New("h1", "s1", "INFO", "batch C message 1234567890", nil)}

	_ = s.Push(batchA)
	time.Sleep(10 * time.Millisecond)
	_ = s.Push(batchB)
	time.Sleep(10 * time.Millisecond)
	_ = s.Push(batchC)

	// Since maxSizeBytes is 800, batchA should have been pruned to stay within limit
	popped, err := s.Pop()
	if err != nil {
		t.Fatalf("Pop() failed: %v", err)
	}
	// The oldest remaining batch should be B
	if popped[0].Message != "batch B message 1234567890" {
		t.Fatalf("expected batch B, got %s", popped[0].Message)
	}
}
