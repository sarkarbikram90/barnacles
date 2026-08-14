// Package spool provides a durable, FIFO disk-backed spool for buffering
// log batches locally when downstream ingestion is temporarily unavailable.
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
	// ErrEmpty is returned when attempting to pop from an empty spool.
	ErrEmpty = errors.New("spool is empty")
	// ErrClosed is returned when operations are performed on a closed spool.
	ErrClosed = errors.New("spool is closed")
)

// Spool manages local disk persistence for unsent log batches.
type Spool struct {
	dir         string
	maxSizeBytes int64
	mu          sync.Mutex
	closed      bool
	currentSize int64
	totalEvents int
	files       []spoolFile
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
// and enforces disk limits.
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
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "spool_") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		fullPath := filepath.Join(s.dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Read header to count events
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

// Push writes a batch of events durably to a new spool segment file.
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

	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		return fmt.Errorf("write temp spool file: %w", err)
	}

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

// Pop retrieves and removes the oldest buffered batch of events.
func (s *Spool) Pop() ([]logentry.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if len(s.files) == 0 {
		return nil, ErrEmpty
	}

	oldest := s.files[0]
	s.files = s.files[1:]
	s.currentSize -= oldest.sizeBytes
	s.totalEvents -= oldest.eventNum

	data, err := os.ReadFile(oldest.path)
	_ = os.Remove(oldest.path)
	if err != nil {
		return nil, fmt.Errorf("read spool file: %w", err)
	}

	var batch StoredBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("unmarshal spool file: %w", err)
	}

	return batch.Events, nil
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
		s.files = s.files[1:]
		s.currentSize -= oldest.sizeBytes
		s.totalEvents -= oldest.eventNum
		_ = os.Remove(oldest.path)
	}
}

// Close marks the spool as closed.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
