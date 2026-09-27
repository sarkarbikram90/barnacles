package spool

import (
	"errors"
	"os"
	"path/filepath"
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

func TestSpoolPeekCommit(t *testing.T) {
	tempDir := t.TempDir()
	s, err := New(tempDir, 10)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer s.Close()

	events := []logentry.LogEntry{
		logentry.New("h1", "app", "INFO", "durable event 1", nil),
	}
	if err := s.Push(events); err != nil {
		t.Fatalf("Push() failed: %v", err)
	}

	// Peek: segment should be marked .inflight on disk but NOT deleted
	lease, err := s.Peek()
	if err != nil {
		t.Fatalf("Peek() failed: %v", err)
	}
	if len(lease.Events) != 1 || lease.Events[0].Message != "durable event 1" {
		t.Fatalf("unexpected leased events: %+v", lease.Events)
	}

	// Verifying underlying file exists as .inflight on disk
	if _, err := os.Stat(lease.file.path); err != nil {
		t.Fatalf("expected in-flight file on disk: %v", err)
	}

	// Second Peek while lease is active should return ErrInFlight
	_, err = s.Peek()
	if !errors.Is(err, ErrInFlight) {
		t.Fatalf("expected ErrInFlight, got %v", err)
	}

	// Commit: file should now be deleted
	path := lease.file.path
	if err := lease.Commit(); err != nil {
		t.Fatalf("Commit() failed: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file to be unlinked after Commit, but it still exists")
	}

	if count := s.Count(); count != 0 {
		t.Fatalf("expected 0 files after commit, got %d", count)
	}
}

func TestSpoolPeekRevertFIFO(t *testing.T) {
	tempDir := t.TempDir()
	s, err := New(tempDir, 10)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer s.Close()

	batch1 := []logentry.LogEntry{logentry.New("h1", "app", "INFO", "batch 1", nil)}
	batch2 := []logentry.LogEntry{logentry.New("h1", "app", "INFO", "batch 2", nil)}
	batch3 := []logentry.LogEntry{logentry.New("h1", "app", "INFO", "batch 3", nil)}

	_ = s.Push(batch1)
	time.Sleep(10 * time.Millisecond)
	_ = s.Push(batch2)

	// Peek batch 1
	lease1, err := s.Peek()
	if err != nil {
		t.Fatalf("Peek() failed: %v", err)
	}
	if lease1.Events[0].Message != "batch 1" {
		t.Fatalf("expected batch 1, got %s", lease1.Events[0].Message)
	}

	// Live batch 3 arrives while batch 1 is in-flight
	time.Sleep(10 * time.Millisecond)
	_ = s.Push(batch3)

	// Delivery of batch 1 fails: REVERT
	if err := lease1.Revert(); err != nil {
		t.Fatalf("Revert() failed: %v", err)
	}

	// Strict FIFO Verification: next Peek MUST return batch 1 again, NOT batch 2 or batch 3!
	leaseRetry, err := s.Peek()
	if err != nil {
		t.Fatalf("Peek() after revert failed: %v", err)
	}
	if leaseRetry.Events[0].Message != "batch 1" {
		t.Fatalf("STRICT FIFO VIOLATED: expected batch 1 after revert, got %s", leaseRetry.Events[0].Message)
	}
	_ = leaseRetry.Commit()

	// Next should be batch 2
	lease2, _ := s.Peek()
	if lease2.Events[0].Message != "batch 2" {
		t.Fatalf("expected batch 2, got %s", lease2.Events[0].Message)
	}
	_ = lease2.Commit()

	// Next should be batch 3
	lease3, _ := s.Peek()
	if lease3.Events[0].Message != "batch 3" {
		t.Fatalf("expected batch 3, got %s", lease3.Events[0].Message)
	}
	_ = lease3.Commit()
}

func TestSpoolCrashRecoveryDuringInFlight(t *testing.T) {
	tempDir := t.TempDir()

	// Phase 1: create spool, push batch, and Peek() it
	s1, err := New(tempDir, 10)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	events := []logentry.LogEntry{
		logentry.New("h1", "app", "FATAL", "in-flight crash event", nil),
	}
	if err := s1.Push(events); err != nil {
		t.Fatalf("Push() failed: %v", err)
	}

	lease, err := s1.Peek()
	if err != nil {
		t.Fatalf("Peek() failed: %v", err)
	}
	// Simulate abrupt crash/SIGKILL: do NOT commit or revert lease, just terminate
	// (Under real SIGKILL or power outage, OS terminates without Close())
	// In-flight file remains on disk with .inflight suffix.
	inFlightPath := lease.file.path
	if _, err := os.Stat(inFlightPath); err != nil {
		t.Fatalf("expected in-flight file: %v", err)
	}

	// Phase 2: Start new agent process on same directory
	s2, err := New(tempDir, 10)
	if err != nil {
		t.Fatalf("New() on recovery failed: %v", err)
	}
	defer s2.Close()

	if count := s2.Count(); count != 1 {
		t.Fatalf("expected 1 recovered file, got %d", count)
	}

	// Peek on recovered spool MUST return the uncommitted batch
	recoveredLease, err := s2.Peek()
	if err != nil {
		t.Fatalf("Peek() after recovery failed: %v", err)
	}
	if len(recoveredLease.Events) != 1 || recoveredLease.Events[0].Message != "in-flight crash event" {
		t.Fatalf("unexpected recovered event: %+v", recoveredLease.Events)
	}

	// Clean commit after recovery
	if err := recoveredLease.Commit(); err != nil {
		t.Fatalf("Commit() on recovered batch failed: %v", err)
	}
	if count := s2.Count(); count != 0 {
		t.Fatalf("expected 0 files after final commit, got %d", count)
	}
}

func TestSpoolSurvivesOrphanTmpFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Write an orphaned .tmp file (simulating power failure mid-Push)
	orphanTmp := filepath.Join(tempDir, "spool_00000000000000000001_deadbeef.tmp")
	_ = os.WriteFile(orphanTmp, []byte("partial corrupted json..."), 0o600)

	s, err := New(tempDir, 10)
	if err != nil {
		t.Fatalf("New() should succeed and clean orphan tmp files: %v", err)
	}
	defer s.Close()

	// Orphan tmp should have been cleaned
	if _, err := os.Stat(orphanTmp); !os.IsNotExist(err) {
		t.Fatalf("expected orphan .tmp file to be removed, but still exists")
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

	// Since maxSizeBytes is 650, batchA should have been pruned to stay within limit
	popped, err := s.Pop()
	if err != nil {
		t.Fatalf("Pop() failed: %v", err)
	}
	// The oldest remaining batch should be B
	if popped[0].Message != "batch B message 1234567890" {
		t.Fatalf("expected batch B, got %s", popped[0].Message)
	}
}
