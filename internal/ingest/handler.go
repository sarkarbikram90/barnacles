package ingest

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
	"github.com/sarkarbikram90/barnacles/internal/store"
	"github.com/sarkarbikram90/barnacles/internal/stream"
)

// Handler processes log ingestion HTTP requests.
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

	var req logentry.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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

	var (
		accepted   []logentry.LogEntry
		duplicates int
		valErrors  []string
	)

	for i := range req.Events {
		entry := &req.Events[i]

		// Default host from Agent ID if missing
		if entry.Host == "" && req.AgentID != "" {
			entry.Host = req.AgentID
		}

		// Validate entry fields & size
		if err := entry.Validate(h.cfg.MaxMessageBytes); err != nil {
			valErrors = append(valErrors, err.Error())
			continue
		}

		// Check idempotency deduplication
		if h.dedup.IsDuplicate(entry.ID) {
			duplicates++
			continue
		}

		accepted = append(accepted, *entry)
	}

	// Persist to store if we have accepted entries
	if len(accepted) > 0 {
		if err := h.store.Append(r.Context(), accepted); err != nil {
			if h.metrics != nil {
				h.metrics.IngestErrorsTotal.Inc()
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
				Status: "error",
				Errors: []string{"storage error: " + err.Error()},
			})
			return
		}

		if h.metrics != nil {
			h.metrics.EventsStoredTotal.Add(float64(len(accepted)))
			h.metrics.StorageBytes.Set(float64(h.store.DiskUsage()))
		}

		// Broadcast to WebSocket clients
		if h.hub != nil {
			h.hub.Broadcast(accepted)
		}
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
