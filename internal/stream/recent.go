// Package stream provides real-time WebSocket distribution of log entries,
// bounded per-client buffering, slow-client protection, and recent-event ring buffering.
package stream

import (
	"sync"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

// RecentBuffer is a thread-safe, bounded ring buffer for recent log entries.
type RecentBuffer struct {
	mu       sync.RWMutex
	capacity int
	entries  []logentry.LogEntry
	head     int
	full     bool
}

// NewRecentBuffer creates a new RecentBuffer with the given maximum capacity.
func NewRecentBuffer(capacity int) *RecentBuffer {
	if capacity <= 0 {
		capacity = 1000
	}
	return &RecentBuffer{
		capacity: capacity,
		entries:  make([]logentry.LogEntry, capacity),
	}
}

// Add appends an entry to the ring buffer.
func (b *RecentBuffer) Add(entry logentry.LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries[b.head] = entry
	b.head = (b.head + 1) % b.capacity
	if b.head == 0 {
		b.full = true
	}
}

// AddBatch appends a batch of entries to the ring buffer.
func (b *RecentBuffer) AddBatch(entries []logentry.LogEntry) {
	if len(entries) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, e := range entries {
		b.entries[b.head] = e
		b.head = (b.head + 1) % b.capacity
		if b.head == 0 {
			b.full = true
		}
	}
}

// Snapshot returns a copy of all recent entries in chronological order (oldest to newest).
func (b *RecentBuffer) Snapshot(limit int) []logentry.LogEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var total int
	if b.full {
		total = b.capacity
	} else {
		total = b.head
	}

	if total == 0 {
		return []logentry.LogEntry{}
	}

	if limit <= 0 || limit > total {
		limit = total
	}

	result := make([]logentry.LogEntry, total)
	if b.full {
		// Oldest is from b.head to end, then 0 to b.head
		n1 := copy(result, b.entries[b.head:])
		copy(result[n1:], b.entries[:b.head])
	} else {
		copy(result, b.entries[:b.head])
	}

	if len(result) > limit {
		// Return the latest `limit` entries
		result = result[len(result)-limit:]
	}

	return result
}

// Len returns the current number of buffered entries.
func (b *RecentBuffer) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.full {
		return b.capacity
	}
	return b.head
}
