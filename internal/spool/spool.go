// Package spool provides a durable, crash-resilient FIFO disk-backed spool for buffering
// log batches locally when downstream ingestion is temporarily unavailable.
// It implements a non-destructive Peek -> Send -> Commit protocol to guarantee
// at-least-once delivery without data loss across process crashes, SIGKILL, and power cuts.
package spool

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

var (
	// ErrEmpty is returned when attempting to peek or pop from an empty spool.
	ErrEmpty = errors.New("spool is empty")
	// ErrClosed is returned when operations are performed on a closed spool.
	ErrClosed = errors.New("spool is closed")
	// ErrInFlight is returned when a batch is already leased and awaiting commit or revert.
	ErrInFlight = errors.New("a batch lease is already in-flight")
	// ErrInvalidLease is returned when attempting to commit or revert an unowned lease.
	ErrInvalidLease = errors.New("lease is invalid or already resolved")
)

// BatchLease represents an in-flight batch leased from the spool.
// The underlying segment file is NOT deleted until Commit() is explicitly called.
type BatchLease struct {
	ID        string              `json:"id"`
	Timestamp time.Time           `json:"timestamp"`
	Events    []logentry.LogEntry `json:"events"`
	file      spoolFile
	spool     *Spool
}

// Commit acknowledges successful delivery of the batch and unlinks the segment file from disk.
func (l *BatchLease) Commit() error {
	if l == nil || l.spool == nil {
		return ErrInvalidLease
	}
	return l.spool.commitLease(l)
}

// Revert returns the batch to the head of the spool queue so it is retried next,
// preserving strict FIFO ordering across retryable network failures.
func (l *BatchLease) Revert() error {
	if l == nil || l.spool == nil {
		return ErrInvalidLease
	}
	return l.spool.revertLease(l)
}

// Spool manages local disk persistence for unsent log batches.
type Spool struct {
	dir          string
	maxSizeBytes int64
	mu           sync.Mutex
	closed       bool
	currentSize  int64
	totalEvents  int
	files        []spoolFile
	activeLease  *BatchLease
}

type spoolFile struct {
	path      string
	sizeBytes int64
	eventNum  int
}

// StoredBatch is the JSON envelope stored in a spool segment file.
type StoredBatch struct {
	ID        string              `json:"id"`
	Timestamp time.Time           `json:"timestamp"`
	Events    []logentry.LogEntry `json:"events"`
}

// New initializes the spool directory, restores any existing spool segments,
// recovers uncommitted in-flight segments from previous crashes, and enforces disk limits.
func New(dir string, maxSizeMB int) (*Spool, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("spool directory cannot be empty")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}

	maxBytes := int64(maxSizeMB) * 1024 * 1024
	if maxBytes <= 0 {
		maxBytes = 1024 * 1024 * 1024 // 1GB default
	}

	s := &Spool{
		dir:          dir,
		maxSizeBytes: maxBytes,
	}

	if err := s.scanExisting(); err != nil {
		return nil, fmt.Errorf("scan existing spool files: %w", err)
	}

	return s, nil
}

func (s *Spool) scanExisting() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	var found []spoolFile
	var totalBytes int64
	var totalEvents int

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "spool_") {
			continue
		}

		fullPath := filepath.Join(s.dir, entry.Name())

		// Crash Recovery: If a previous process died while a batch was in-flight,
		// recover it back to pending status so it will be retried at the head of the queue.
		if strings.HasSuffix(entry.Name(), ".inflight") {
			restoredPath := strings.TrimSuffix(fullPath, ".inflight") + ".json"
			if err := os.Rename(fullPath, restoredPath); err == nil {
				fullPath = restoredPath
			}
		} else if !strings.HasSuffix(entry.Name(), ".json") {
			// Clean up orphaned .tmp files left from crashes during writes
			if strings.HasSuffix(entry.Name(), ".tmp") {
				_ = os.Remove(fullPath)
			}
			continue
		}

		info, err := os.Stat(fullPath)
		if err != nil {
			continue
		}

		data, err := os.ReadFile(fullPath)
		if err != nil {
			continue
		}
		var batch StoredBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			// Corrupt spool file; remove it
			_ = os.Remove(fullPath)
			continue
		}

		found = append(found, spoolFile{
			path:      fullPath,
			sizeBytes: info.Size(),
			eventNum:  len(batch.Events),
		})
		totalBytes += info.Size()
		totalEvents += len(batch.Events)
	}

	// Sort lexicographically by filename (which starts with timestamp nano) for FIFO ordering
	sort.Slice(found, func(i, j int) bool {
		return filepath.Base(found[i].path) < filepath.Base(found[j].path)
	})

	s.files = found
	s.currentSize = totalBytes
	s.totalEvents = totalEvents

	s.enforceLimitLocked()
	return nil
}

// Push writes a batch of events durably to disk using an fsynced temporary file
// followed by an atomic rename into a pending segment file.
func (s *Spool) Push(events []logentry.LogEntry) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}

	batch := StoredBatch{
		ID:        uuid.NewString(),
		Timestamp: time.Now().UTC(),
		Events:    events,
	}

	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal spool batch: %w", err)
	}

	fileName := fmt.Sprintf("spool_%020d_%s.json", time.Now().UnixNano(), batch.ID)
	finalPath := filepath.Join(s.dir, fileName)
	tempPath := filepath.Join(s.dir, "."+fileName+".tmp")

	// Create and write with fsync for crash durability
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create temp spool file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("write temp spool file: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("fsync temp spool file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close temp spool file: %w", err)
	}

	// Atomic rename to commit to disk
	if err := os.Rename(tempPath, finalPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("rename spool file: %w", err)
	}

	fileSize := int64(len(data))
	s.files = append(s.files, spoolFile{
		path:      finalPath,
		sizeBytes: fileSize,
		eventNum:  len(events),
	})
	s.currentSize += fileSize
	s.totalEvents += len(events)

	s.enforceLimitLocked()
	return nil
}

// Peek retrieves the oldest buffered batch of events without deleting it.
// The segment file is marked as in-flight on disk so uncommitted batches are preserved
// and recovered on crash. Only one lease may be active at a time to preserve strict FIFO.
func (s *Spool) Peek() (*BatchLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrClosed
	}
	if s.activeLease != nil {
		return nil, ErrInFlight
	}
	if len(s.files) == 0 {
		return nil, ErrEmpty
	}

	oldest := s.files[0]

	// Mark segment in-flight on disk
	inflightPath := strings.TrimSuffix(oldest.path, ".json") + ".inflight"
	if err := os.Rename(oldest.path, inflightPath); err != nil {
		return nil, fmt.Errorf("mark spool segment in-flight: %w", err)
	}

	oldest.path = inflightPath
	s.files[0] = oldest

	data, err := os.ReadFile(inflightPath)
	if err != nil {
		// Revert rename if read fails
		normalPath := strings.TrimSuffix(inflightPath, ".inflight") + ".json"
		_ = os.Rename(inflightPath, normalPath)
		oldest.path = normalPath
		s.files[0] = oldest
		return nil, fmt.Errorf("read spool file: %w", err)
	}

	var batch StoredBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		// Corrupt batch: remove and advance
		_ = os.Remove(inflightPath)
		s.files = s.files[1:]
		s.currentSize -= oldest.sizeBytes
		s.totalEvents -= oldest.eventNum
		return nil, fmt.Errorf("unmarshal spool batch: %w", err)
	}

	lease := &BatchLease{
		ID:        batch.ID,
		Timestamp: batch.Timestamp,
		Events:    batch.Events,
		file:      oldest,
		spool:     s,
	}
	s.activeLease = lease
	return lease, nil
}

func (s *Spool) commitLease(l *BatchLease) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeLease != l {
		return ErrInvalidLease
	}

	// Delete segment file from disk now that receipt is confirmed
	_ = os.Remove(l.file.path)

	// Remove from tracked files
	if len(s.files) > 0 && s.files[0].path == l.file.path {
		s.files = s.files[1:]
	} else {
		for i, f := range s.files {
			if f.path == l.file.path {
				s.files = append(s.files[:i], s.files[i+1:]...)
				break
			}
		}
	}

	s.currentSize -= l.file.sizeBytes
	s.totalEvents -= l.file.eventNum
	s.activeLease = nil
	return nil
}

func (s *Spool) revertLease(l *BatchLease) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeLease != l {
		return ErrInvalidLease
	}

	// Revert file on disk from .inflight back to .json
	normalPath := strings.TrimSuffix(l.file.path, ".inflight") + ".json"
	if err := os.Rename(l.file.path, normalPath); err != nil {
		return fmt.Errorf("revert spool segment on disk: %w", err)
	}

	l.file.path = normalPath
	if len(s.files) > 0 && s.files[0].path == strings.TrimSuffix(normalPath, ".json")+".inflight" {
		s.files[0] = l.file
	} else {
		for i, f := range s.files {
			if f.path == strings.TrimSuffix(normalPath, ".json")+".inflight" {
				s.files[i] = l.file
				break
			}
		}
	}

	s.activeLease = nil
	return nil
}

// Pop retrieves and removes the oldest buffered batch of events atomically.
// Maintained for backward compatibility. In production, prefer Peek() -> Commit().
func (s *Spool) Pop() ([]logentry.LogEntry, error) {
	lease, err := s.Peek()
	if err != nil {
		return nil, err
	}
	events := lease.Events
	if err := lease.Commit(); err != nil {
		return nil, fmt.Errorf("commit popped lease: %w", err)
	}
	return events, nil
}

// Size returns the current disk usage in bytes and number of queued events.
func (s *Spool) Size() (bytes int64, events int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentSize, s.totalEvents
}

// Count returns the number of segment files queued.
func (s *Spool) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.files)
}

func (s *Spool) enforceLimitLocked() {
	for s.currentSize > s.maxSizeBytes && len(s.files) > 0 {
		oldest := s.files[0]
		// Do not prune the currently active in-flight lease
		if s.activeLease != nil && s.activeLease.file.path == oldest.path {
			if len(s.files) > 1 {
				oldest = s.files[1]
				s.files = append(s.files[:1], s.files[2:]...)
			} else {
				break
			}
		} else {
			s.files = s.files[1:]
		}

		s.currentSize -= oldest.sizeBytes
		s.totalEvents -= oldest.eventNum
		_ = os.Remove(oldest.path)
	}
}

// Close marks the spool as closed. If a lease is in-flight, it is safely reverted.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeLease != nil {
		normalPath := strings.TrimSuffix(s.activeLease.file.path, ".inflight") + ".json"
		_ = os.Rename(s.activeLease.file.path, normalPath)
		s.activeLease = nil
	}
	s.closed = true
	return nil
}
