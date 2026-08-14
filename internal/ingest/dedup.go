// Package ingest handles incoming log batch HTTP requests, validation,
// idempotency deduplication, persistence to storage, and live streaming dispatch.
package ingest

import (
	"sync"
	"time"
)

// DedupCache is an in-memory, bounded TTL cache to prevent duplicate processing of log entries.
type DedupCache struct {
	mu       sync.Mutex
	window   time.Duration
	capacity int
	entries  map[string]time.Time
}

// NewDedupCache creates a new DedupCache with specified TTL window and maximum capacity.
func NewDedupCache(window time.Duration, capacity int) *DedupCache {
	if window <= 0 {
		window = 5 * time.Minute
	}
	if capacity <= 0 {
		capacity = 50000
	}
	return &DedupCache{
		window:   window,
		capacity: capacity,
		entries:  make(map[string]time.Time, 1024),
	}
}

// IsDuplicate returns true if the ID was already recorded within the TTL window.
// If not a duplicate, it adds the ID to the cache.
func (c *DedupCache) IsDuplicate(id string) bool {
	if id == "" {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Check if already seen
	if seenTime, exists := c.entries[id]; exists {
		if now.Sub(seenTime) <= c.window {
			return true
		}
	}

	// If cache exceeds capacity, prune expired entries
	if len(c.entries) >= c.capacity {
		c.pruneExpiredLocked(now)
		// If still over capacity, evict random/arbitrary batch
		if len(c.entries) >= c.capacity {
			c.evictOldestLocked()
		}
	}

	c.entries[id] = now
	return false
}

func (c *DedupCache) pruneExpiredLocked(now time.Time) {
	for k, v := range c.entries {
		if now.Sub(v) > c.window {
			delete(c.entries, k)
		}
	}
}

func (c *DedupCache) evictOldestLocked() {
	// Evict ~10% of entries when capacity limit is reached
	count := 0
	limit := c.capacity / 10
	if limit == 0 {
		limit = 1
	}
	for k := range c.entries {
		delete(c.entries, k)
		count++
		if count >= limit {
			break
		}
	}
}

// Len returns the current count of cached IDs.
func (c *DedupCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
