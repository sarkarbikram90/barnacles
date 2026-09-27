// Package tailer provides robust file tailing and watermark checkpointing.
package tailer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Checkpoint stores the persistent watermark state of a watched log file.
type Checkpoint struct {
	DeviceID          uint64    `json:"device_id"`
	FileIdentity      uint64    `json:"file_identity"`
	FilePath          string    `json:"file_path"`
	ByteOffset        int64     `json:"byte_offset"`
	LastSeenTimestamp time.Time `json:"last_seen_timestamp"`
}

// CheckpointRegistry manages thread-safe, durable state persistence for tailer offsets.
type CheckpointRegistry struct {
	path    string
	mu      sync.RWMutex
	entries map[string]Checkpoint
	dirty   bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
	closed  bool
}

// NewCheckpointRegistry loads any existing checkpoint registry from disk or initializes a new one.
// If flushInterval > 0, a background worker periodically persists dirty offsets to disk.
func NewCheckpointRegistry(path string, flushInterval time.Duration) (*CheckpointRegistry, error) {
	if path == "" {
		return nil, errors.New("checkpoint registry path cannot be empty")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create checkpoint directory %q: %w", dir, err)
	}

	r := &CheckpointRegistry{
		path:    path,
		entries: make(map[string]Checkpoint),
		stopCh:  make(chan struct{}),
	}

	if err := r.load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("load checkpoint registry: %w", err)
	}

	if flushInterval > 0 {
		r.wg.Add(1)
		go r.flusher(flushInterval)
	}

	return r, nil
}

func (r *CheckpointRegistry) makeKey(deviceID, fileIdentity uint64) string {
	return fmt.Sprintf("%d:%d", deviceID, fileIdentity)
}

func (r *CheckpointRegistry) load() error {
	data, err := os.ReadFile(r.path)
	if err != nil {
		return err
	}

	var stored map[string]Checkpoint
	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("unmarshal checkpoints: %w", err)
	}

	r.entries = stored
	return nil
}

// Get retrieves the checkpoint for a specific file identity.
func (r *CheckpointRegistry) Get(deviceID, fileIdentity uint64) (Checkpoint, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := r.makeKey(deviceID, fileIdentity)
	cp, exists := r.entries[key]
	return cp, exists
}

// GetByPath retrieves the checkpoint by file path as a fallback when inode/device is unavailable.
func (r *CheckpointRegistry) GetByPath(path string) (Checkpoint, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, cp := range r.entries {
		if cp.FilePath == path {
			return cp, true
		}
	}
	return Checkpoint{}, false
}

// Set records or updates a checkpoint in memory and marks the registry dirty.
func (r *CheckpointRegistry) Set(cp Checkpoint) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if cp.LastSeenTimestamp.IsZero() {
		cp.LastSeenTimestamp = time.Now().UTC()
	}

	key := r.makeKey(cp.DeviceID, cp.FileIdentity)
	r.entries[key] = cp
	r.dirty = true
}

// Save writes all recorded checkpoints to disk durably using atomic file renaming.
func (r *CheckpointRegistry) Save() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.dirty && len(r.entries) > 0 {
		return nil
	}

	data, err := json.MarshalIndent(r.entries, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoints: %w", err)
	}

	dir := filepath.Dir(r.path)
	base := filepath.Base(r.path)
	tempPath := filepath.Join(dir, fmt.Sprintf(".%s.%d.tmp", base, time.Now().UnixNano()))

	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create temp checkpoint file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("write temp checkpoint file: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("fsync checkpoint file: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close checkpoint file: %w", err)
	}

	if err := os.Rename(tempPath, r.path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("atomic rename checkpoint file: %w", err)
	}

	r.dirty = false
	return nil
}

func (r *CheckpointRegistry) flusher(interval time.Duration) {
	defer r.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			_ = r.Save()
			return
		case <-ticker.C:
			_ = r.Save()
		}
	}
}

// Count returns the number of checkpoints tracked.
func (r *CheckpointRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// Close gracefully flushes all dirty checkpoints and stops the flusher worker.
func (r *CheckpointRegistry) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.stopCh)
	r.mu.Unlock()

	r.wg.Wait()
	return r.Save()
}
