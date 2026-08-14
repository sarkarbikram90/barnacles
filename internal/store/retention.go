package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
)

// RetentionWorker executes periodic background pruning on a LogStore.
type RetentionWorker struct {
	store  LogStore
	cfg    config.RetentionSettings
	stopCh chan struct{}
}

// NewRetentionWorker creates a RetentionWorker.
func NewRetentionWorker(store LogStore, cfg config.RetentionSettings) *RetentionWorker {
	return &RetentionWorker{
		store:  store,
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Start begins the background retention loop. Exits when ctx is cancelled or Stop is called.
func (w *RetentionWorker) Start(ctx context.Context) {
	if !w.cfg.Enabled {
		return
	}

	interval := w.cfg.CheckInterval
	if interval <= 0 {
		interval = 10 * time.Minute
	}

	maxAge := time.Duration(w.cfg.MaxAgeHours) * time.Hour
	maxSizeBytes := int64(w.cfg.MaxSizeGB) * 1024 * 1024 * 1024

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			deleted, freed, err := w.store.Prune(ctx, maxAge, maxSizeBytes)
			if err != nil {
				slog.Error("Retention worker pruning error", "error", err)
			} else if deleted > 0 {
				slog.Info("Retention worker pruned logs", "deleted_files", deleted, "freed_bytes", freed)
			}
		}
	}
}

// Stop signals the worker to exit.
func (w *RetentionWorker) Stop() {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
}
