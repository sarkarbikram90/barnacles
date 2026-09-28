package ingest

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
	"github.com/sarkarbikram90/barnacles/internal/store"
	"github.com/sarkarbikram90/barnacles/internal/stream"
)

func decompressReader(r io.Reader, encoding string) (io.Reader, func(), error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "zstd":
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, nil, fmt.Errorf("init zstd reader: %w", err)
		}
		return zr, zr.Close, nil
	case "gzip":
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, fmt.Errorf("init gzip reader: %w", err)
		}
		return gr, func() { _ = gr.Close() }, nil
	case "", "identity":
		return r, func() {}, nil
	default:
		return nil, nil, fmt.Errorf("unsupported content encoding: %s", encoding)
	}
}

// Handler processes log ingestion HTTP requests for both Barnacles batch API and OpenTelemetry /v1/logs.
type Handler struct {
	cfg     config.IngestSettings
	store   store.LogStore
	hub     *stream.Hub
	metrics *metrics.ServerMetrics
	dedup   *DedupCache
}

// NewHandler creates a new Ingest HTTP handler.
func NewHandler(
	cfg config.IngestSettings,
	st store.LogStore,
	hub *stream.Hub,
	m *metrics.ServerMetrics,
) *Handler {
	dedup := NewDedupCache(cfg.DedupWindow, cfg.DedupCapacity)
	return &Handler{
		cfg:     cfg,
		store:   st,
		hub:     hub,
		metrics: m,
		dedup:   dedup,
	}
}

// ServeHTTP handles POST /api/v1/ingest.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		if h.metrics != nil {
			h.metrics.IngestDuration.Observe(time.Since(start).Seconds())
		}
	}()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limit request body size
	maxBytes := int64(10 * 1024 * 1024) // 10MB default
	if h.cfg.MaxMessageBytes > 0 {
		maxBytes = int64(h.cfg.MaxBatchEvents * h.cfg.MaxMessageBytes)
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	decompressedBody, closeDecompressor, err := decompressReader(r.Body, r.Header.Get("Content-Encoding"))
	if err != nil {
		if h.metrics != nil {
			h.metrics.IngestErrorsTotal.Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status: "error",
			Errors: []string{"decompression error: " + err.Error()},
		})
		return
	}
	defer closeDecompressor()

	var req logentry.IngestRequest
	if err := json.NewDecoder(io.LimitReader(decompressedBody, 50*1024*1024)).Decode(&req); err != nil {
		if h.metrics != nil {
			h.metrics.IngestErrorsTotal.Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status: "error",
			Errors: []string{"invalid JSON payload or body too large: " + err.Error()},
		})
		return
	}

	if len(req.Events) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status:   "ok",
			Accepted: 0,
		})
		return
	}

	if h.cfg.MaxBatchEvents > 0 && len(req.Events) > h.cfg.MaxBatchEvents {
		if h.metrics != nil {
			h.metrics.IngestErrorsTotal.Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status: "error",
			Errors: []string{"batch exceeds maximum allowed event count"},
		})
		return
	}

	if h.metrics != nil {
		h.metrics.EventsIngestedTotal.Add(float64(len(req.Events)))
	}

	accepted, duplicates, valErrors, err := h.ingestEntries(r.Context(), req.Events, req.AgentID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status: "error",
			Errors: []string{"storage error: " + err.Error()},
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
		Status:     "ok",
		Accepted:   len(accepted),
		Duplicates: duplicates,
		Errors:     valErrors,
	})
}

// ServeOTLP handles standard OpenTelemetry OTLP/HTTP POST /v1/logs requests.
func (h *Handler) ServeOTLP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		if h.metrics != nil {
			h.metrics.IngestDuration.Observe(time.Since(start).Seconds())
		}
	}()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	maxBytes := int64(10 * 1024 * 1024)
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	decompressedBody, closeDecompressor, err := decompressReader(r.Body, r.Header.Get("Content-Encoding"))
	if err != nil {
		if h.metrics != nil {
			h.metrics.IngestErrorsTotal.Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "decompression error: " + err.Error()})
		return
	}
	defer closeDecompressor()

	entries, err := ParseOTLPLogsJSON(io.LimitReader(decompressedBody, 50*1024*1024))
	if err != nil {
		if h.metrics != nil {
			h.metrics.IngestErrorsTotal.Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	if len(entries) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"partialSuccess": map[string]any{}})
		return
	}

	if h.metrics != nil {
		h.metrics.EventsIngestedTotal.Add(float64(len(entries)))
	}

	accepted, _, _, err := h.ingestEntries(r.Context(), entries, "otlp-service")
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"partialSuccess": map[string]any{
			"rejectedLogRecords": len(entries) - len(accepted),
		},
	})
}

func (h *Handler) ingestEntries(ctx context.Context, entries []logentry.LogEntry, defaultHost string) ([]logentry.LogEntry, int, []string, error) {
	var (
		accepted   []logentry.LogEntry
		duplicates int
		valErrors  []string
	)

	for i := range entries {
		entry := &entries[i]

		if entry.Host == "" && defaultHost != "" {
			entry.Host = defaultHost
		}

		if err := entry.Validate(h.cfg.MaxMessageBytes); err != nil {
			valErrors = append(valErrors, err.Error())
			continue
		}

		if h.dedup.IsDuplicate(entry.ID) {
			duplicates++
			continue
		}

		accepted = append(accepted, *entry)
	}

	if len(accepted) > 0 {
		if err := h.store.Append(ctx, accepted); err != nil {
			if h.metrics != nil {
				h.metrics.IngestErrorsTotal.Inc()
			}
			return nil, duplicates, valErrors, err
		}

		if h.metrics != nil {
			h.metrics.EventsStoredTotal.Add(float64(len(accepted)))
			h.metrics.StorageBytes.Set(float64(h.store.DiskUsage()))
		}

		if h.hub != nil {
			h.hub.Broadcast(accepted)
		}
	}

	return accepted, duplicates, valErrors, nil
}
