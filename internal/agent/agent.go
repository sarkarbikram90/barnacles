// Package agent orchestrates file tailers, parsers, batching, disk spooling,
// and resilient HTTP transmission to the central Barnacles server.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
	"github.com/sarkarbikram90/barnacles/internal/parser"
	"github.com/sarkarbikram90/barnacles/internal/sender"
	"github.com/sarkarbikram90/barnacles/internal/spool"
	"github.com/sarkarbikram90/barnacles/internal/tailer"
)

// Agent coordinates log collection and delivery on a host.
type Agent struct {
	cfg         config.AgentConfig
	metrics     *metrics.AgentMetrics
	sender      *sender.Sender
	spool       *spool.Spool
	checkpoints *tailer.CheckpointRegistry
	logCh       chan logentry.LogEntry
	tailers     []*tailer.Tailer
	managers    []*tailer.TailerManager
	wg          sync.WaitGroup
}

// New creates and configures a new Agent instance.
func New(cfg config.AgentConfig, m *metrics.AgentMetrics) (*Agent, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate agent config: %w", err)
	}

	if m == nil {
		m = metrics.NewAgentMetrics()
	}

	snd, err := sender.New(sender.Config{
		URL:                cfg.Server.URL,
		Token:              cfg.Server.Token,
		Timeout:            cfg.Server.Timeout,
		InsecureSkipVerify: cfg.Server.InsecureSkipVerify,
		Compression:        cfg.Server.Compression,
		Format:             cfg.Server.Format,
	})
	if err != nil {
		return nil, fmt.Errorf("create sender: %w", err)
	}

	var sp *spool.Spool
	var cpReg *tailer.CheckpointRegistry
	if cfg.Spool.Enabled {
		s, err := spool.New(cfg.Spool.Directory, cfg.Spool.MaxSizeMB)
		if err != nil {
			return nil, fmt.Errorf("create spool: %w", err)
		}
		sp = s
		// Update spool metrics
		bytesUsed, eventCount := sp.Size()
		m.SpoolBytes.Set(float64(bytesUsed))
		m.SpoolEvents.Set(float64(eventCount))

		checkpointFile := filepath.Join(cfg.Spool.Directory, "checkpoints.json")
		c, err := tailer.NewCheckpointRegistry(checkpointFile, 500*time.Millisecond)
		if err != nil {
			return nil, fmt.Errorf("create checkpoint registry: %w", err)
		}
		cpReg = c
	}

	queueCap := cfg.Batch.MaxQueueEvents
	if queueCap <= 0 {
		queueCap = 5000
	}

	return &Agent{
		cfg:         cfg,
		metrics:     m,
		sender:      snd,
		spool:       sp,
		checkpoints: cpReg,
		logCh:       make(chan logentry.LogEntry, queueCap),
	}, nil
}

// Start begins file tailing, log batching, and transmission workers.
// Blocks until ctx is cancelled and all workers have shutdown gracefully.
func (a *Agent) Start(ctx context.Context) error {
	slog.Info("Starting Barnacles agent", "id", a.cfg.Agent.ID, "sources", len(a.cfg.Sources))

	a.metrics.SourcesActive.Set(float64(len(a.cfg.Sources)))

	// Start tailers for each configured source
	for _, srcCfg := range a.cfg.Sources {
		p, err := parser.New(srcCfg.Format, srcCfg.Pattern, a.cfg.Agent.Host, srcCfg.Name, srcCfg.Tags)
		if err != nil {
			return fmt.Errorf("create parser for source %q: %w", srcCfg.Name, err)
		}

		if strings.ContainsAny(srcCfg.Path, "*?[]") {
			mgr, err := tailer.NewManager(ctx, tailer.ManagerConfig{
				Pattern:            srcCfg.Path,
				PollInterval:       1 * time.Second,
				TailerPollInterval: 50 * time.Millisecond,
				StartPosition:      srcCfg.StartPosition,
				CheckpointRegistry: a.checkpoints,
			})
			if err != nil {
				return fmt.Errorf("start glob tailer manager for source %q: %w", srcCfg.Name, err)
			}
			a.managers = append(a.managers, mgr)

			a.wg.Add(1)
			go a.managerWorker(ctx, srcCfg.Name, mgr, p)
		} else {
			t, err := tailer.New(ctx, tailer.Config{
				Path:               srcCfg.Path,
				StartPosition:      srcCfg.StartPosition,
				CheckpointRegistry: a.checkpoints,
			})
			if err != nil {
				return fmt.Errorf("start tailer for source %q: %w", srcCfg.Name, err)
			}
			a.tailers = append(a.tailers, t)

			a.wg.Add(1)
			go a.tailWorker(ctx, srcCfg.Name, t, p)
		}
	}

	// Start batcher / sender worker
	a.wg.Add(1)
	go a.batchWorker(ctx)

	// Start spool drain worker if enabled
	if a.spool != nil {
		a.wg.Add(1)
		go a.spoolDrainWorker(ctx)
	}

	// Wait for context cancellation
	<-ctx.Done()
	slog.Info("Shutting down Barnacles agent...")

	// Close all single-file tailers and dynamic managers to stop producing new lines
	for _, t := range a.tailers {
		_ = t.Close()
	}
	for _, mgr := range a.managers {
		_ = mgr.Close()
	}

	// Wait for all workers (including in-flight batch flushes) to complete
	a.wg.Wait()

	if a.checkpoints != nil {
		_ = a.checkpoints.Close()
	}

	if a.spool != nil {
		_ = a.spool.Close()
	}

	slog.Info("Barnacles agent stopped cleanly")
	return nil
}

func (a *Agent) tailWorker(ctx context.Context, sourceName string, t *tailer.Tailer, p parser.Parser) {
	defer a.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-t.Errors():
			if !ok {
				return
			}
			slog.Warn("Tailer warning", "source", sourceName, "error", err)
		case line, ok := <-t.Lines():
			if !ok {
				return
			}
			a.metrics.EventsReadTotal.WithLabelValues(sourceName).Inc()

			entry, err := p.Parse(line)
			if err != nil {
				a.metrics.ParseErrorsTotal.WithLabelValues(sourceName).Inc()
				// Preserve unparseable lines as text with ERROR level or fallback
				entry = logentry.New(a.cfg.Agent.Host, sourceName, "INFO", line, nil)
			}
			a.metrics.EventsParsedTotal.WithLabelValues(sourceName).Inc()

			select {
			case a.logCh <- entry:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (a *Agent) managerWorker(ctx context.Context, sourceName string, mgr *tailer.TailerManager, p parser.Parser) {
	defer a.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-mgr.Errors():
			if !ok {
				return
			}
			slog.Warn("Tailer manager warning", "source", sourceName, "error", err)
		case lineEvt, ok := <-mgr.Lines():
			if !ok {
				return
			}
			a.metrics.EventsReadTotal.WithLabelValues(sourceName).Inc()

			entry, err := p.Parse(lineEvt.Text)
			if err != nil {
				a.metrics.ParseErrorsTotal.WithLabelValues(sourceName).Inc()
				entry = logentry.New(a.cfg.Agent.Host, sourceName, "INFO", lineEvt.Text, nil)
			}
			if entry.Fields == nil {
				entry.Fields = make(map[string]string)
			}
			entry.Fields["file_path"] = lineEvt.Path
			a.metrics.EventsParsedTotal.WithLabelValues(sourceName).Inc()

			select {
			case a.logCh <- entry:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (a *Agent) batchWorker(ctx context.Context) {
	defer a.wg.Done()

	batch := make([]logentry.LogEntry, 0, a.cfg.Batch.MaxEvents)
	ticker := time.NewTicker(a.cfg.Batch.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		a.deliverBatch(ctx, batch)
		batch = make([]logentry.LogEntry, 0, a.cfg.Batch.MaxEvents)
	}

	for {
		select {
		case <-ctx.Done():
			// Drain any remaining buffered logs in logCh
			for {
				select {
				case entry := <-a.logCh:
					batch = append(batch, entry)
					if len(batch) >= a.cfg.Batch.MaxEvents {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case <-ticker.C:
			flush()
		case entry, ok := <-a.logCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, entry)
			if len(batch) >= a.cfg.Batch.MaxEvents {
				flush()
			}
		}
	}
}

func (a *Agent) deliverBatch(ctx context.Context, batch []logentry.LogEntry) {
	if len(batch) == 0 {
		return
	}

	// If spool has pending backlog, do NOT send live batch directly to prevent out-of-order delivery.
	// Enqueue directly to spool tail so strict FIFO ordering is maintained.
	if a.spool != nil && a.spool.Count() > 0 {
		if spoolErr := a.spool.Push(batch); spoolErr != nil {
			slog.Error("Failed to buffer batch to disk spool", "error", spoolErr)
		} else {
			bytesUsed, eventCount := a.spool.Size()
			a.metrics.SpoolBytes.Set(float64(bytesUsed))
			a.metrics.SpoolEvents.Set(float64(eventCount))
		}
		return
	}

	// Try sending directly with a short timeout. If parent ctx is already cancelled
	// (during final graceful shutdown flush), allow a 2-second grace timeout so buffered
	// events are delivered to the server instead of being aborted with context canceled.
	var sendCtx context.Context
	var cancel context.CancelFunc
	if ctx.Err() != nil {
		sendCtx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	} else {
		sendCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	}
	_, err := a.sender.Send(sendCtx, a.cfg.Agent.ID, batch)
	cancel()

	if err == nil {
		a.metrics.BatchesSentTotal.Inc()
		a.metrics.EventsSentTotal.Add(float64(len(batch)))
		return
	}

	a.metrics.SendErrorsTotal.Inc()
	slog.Warn("Failed to send log batch to server", "error", err, "events", len(batch))

	// If spool is enabled and error is retryable or network failure, buffer to spool
	if a.spool != nil && sender.IsRetryable(err) {
		if spoolErr := a.spool.Push(batch); spoolErr != nil {
			slog.Error("Failed to buffer batch to disk spool", "error", spoolErr)
		} else {
			bytesUsed, eventCount := a.spool.Size()
			a.metrics.SpoolBytes.Set(float64(bytesUsed))
			a.metrics.SpoolEvents.Set(float64(eventCount))
			slog.Debug("Buffered batch to disk spool", "queued_events", eventCount)
		}
	}
}

func (a *Agent) spoolDrainWorker(ctx context.Context) {
	defer a.wg.Done()

	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}

		if a.spool.Count() == 0 {
			// Nothing to drain; wait briefly
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}

		// Non-destructive Peek: does NOT delete from disk until confirmed by server ACK
		lease, err := a.spool.Peek()
		if err != nil {
			if errors.Is(err, spool.ErrEmpty) || errors.Is(err, spool.ErrInFlight) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
			slog.Error("Failed to peek batch from spool", "error", err)
			continue
		}

		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, sendErr := a.sender.Send(sendCtx, a.cfg.Agent.ID, lease.Events)
		cancel()

		if sendErr == nil {
			attempt = 0
			// Commit: unlinks segment file from disk only upon verified delivery
			if commitErr := lease.Commit(); commitErr != nil {
				slog.Error("Failed to commit spool lease", "error", commitErr)
			}
			a.metrics.BatchesSentTotal.Inc()
			a.metrics.EventsSentTotal.Add(float64(len(lease.Events)))

			bytesUsed, eventCount := a.spool.Size()
			a.metrics.SpoolBytes.Set(float64(bytesUsed))
			a.metrics.SpoolEvents.Set(float64(eventCount))
			continue
		}

		// Failed to send:
		a.metrics.SendErrorsTotal.Inc()
		if sender.IsRetryable(sendErr) {
			// Revert lease to HEAD of queue so it is retried next, preserving strict FIFO ordering!
			if revertErr := lease.Revert(); revertErr != nil {
				slog.Error("Failed to revert spool lease", "error", revertErr)
			}
		} else {
			// Permanent error (e.g. 400 Bad Request or malformed): commit to drop poison pill
			slog.Error("Dropping poison batch due to permanent non-retryable error", "error", sendErr)
			_ = lease.Commit()
		}

		attempt++
		backoff := sender.CalculateBackoff(
			attempt,
			a.cfg.Retry.InitialBackoff,
			a.cfg.Retry.MaxBackoff,
			a.cfg.Retry.BackoffMultiplier,
		)
		slog.Debug("Spool drain failed; backing off", "backoff", backoff, "attempt", attempt)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}
