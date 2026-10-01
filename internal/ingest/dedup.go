// Package ingest handles incoming log batch HTTP requests, validation,
// idempotency deduplication, persistence to storage, and live streaming dispatch.
package ingest

import (
	"container/list"
	"sync"
	"time"
)

// dedupEntry represents a single tracked event ID in the LRU dedup cache.
type dedupEntry struct {
	id       string
	seenTime time.Time
}

// DedupCache is an in-memory, bounded LRU/TTL cache to prevent duplicate processing of log entries.
// All operations (lookup, insert, eviction) are O(1) amortized.
type DedupCache struct {
	mu       sync.Mutex
	window   time.Duration
	capacity int
	index    map[string]*list.Element // key -> LRU list element
	order    *list.List               // front = newest, back = oldest
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
		index:    make(map[string]*list.Element, 1024),
		order:    list.New(),
	}
}

// IsDuplicate returns true if the ID was already recorded within the TTL window.
// If not a duplicate, it adds the ID to the cache. O(1) amortized.
func (c *DedupCache) IsDuplicate(id string) bool {
	if id == "" {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Check if already seen and still within TTL window
	if elem, exists := c.index[id]; exists {
		entry := elem.Value.(*dedupEntry)
		if now.Sub(entry.seenTime) <= c.window {
			// Move to front (most recently seen)
			entry.seenTime = now
			c.order.MoveToFront(elem)
			return true
		}
		// Entry expired: remove and re-insert below
		c.order.Remove(elem)
		delete(c.index, id)
	}

	// Evict expired entries from the tail (oldest) — O(1) amortized
	c.evictExpiredFromTailLocked(now)

	// If still at capacity after evicting expired, remove the oldest entry
	for c.order.Len() >= c.capacity {
		c.removeTailLocked()
	}

	// Insert new entry at front
	entry := &dedupEntry{id: id, seenTime: now}
	elem := c.order.PushFront(entry)
	c.index[id] = elem

	return false
}

// evictExpiredFromTailLocked removes expired entries from the back of the LRU list.
// Because entries are ordered by recency, once we hit a non-expired entry we can stop.
func (c *DedupCache) evictExpiredFromTailLocked(now time.Time) {
	for {
		tail := c.order.Back()
		if tail == nil {
			return
		}
		entry := tail.Value.(*dedupEntry)
		if now.Sub(entry.seenTime) <= c.window {
			return // Remaining entries are newer; stop
		}
		c.order.Remove(tail)
		delete(c.index, entry.id)
	}
}

// removeTailLocked removes the single oldest entry from the cache.
func (c *DedupCache) removeTailLocked() {
	tail := c.order.Back()
	if tail == nil {
		return
	}
	entry := tail.Value.(*dedupEntry)
	c.order.Remove(tail)
	delete(c.index, entry.id)
}

// Len returns the current count of cached IDs.
func (c *DedupCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
