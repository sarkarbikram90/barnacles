// Package store defines the storage interface and filesystem-backed log persistence
// supporting time-segmented indexing, querying, and retention pruning.
package store

import (
	"context"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

// LogStore defines the persistence contract for Barnacles log storage backends.
type LogStore interface {
	// Append writes a batch of normalized log entries to persistent storage.
	Append(ctx context.Context, entries []logentry.LogEntry) error

	// Query retrieves entries satisfying filtering and time constraints.
	Query(ctx context.Context, q logentry.Query) ([]logentry.LogEntry, error)

	// Sources returns the distinct set of known log source names.
	Sources(ctx context.Context) ([]string, error)

	// Hosts returns the distinct set of known host identifiers.
	Hosts(ctx context.Context) ([]string, error)

	// DiskUsage returns total bytes consumed by log storage on disk.
	DiskUsage() int64

	// Prune deletes stored log segments that exceed max age or disk budget.
	Prune(ctx context.Context, maxAge time.Duration, maxSizeBytes int64) (deletedFiles int, freedBytes int64, err error)

	// Close flushes buffers and closes any open storage file handles.
	Close() error
}
