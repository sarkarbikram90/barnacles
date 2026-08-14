// Package health provides HTTP handlers for liveness (/healthz) and readiness (/readyz) probes.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Checker represents a function that returns an error if a dependency is unhealthy.
type Checker func(ctx context.Context) error

// Handler manages health check status and dependency checks.
type Handler struct {
	startTime time.Time
	mu        sync.RWMutex
	checkers  map[string]Checker
}

// NewHandler creates a new health check Handler.
func NewHandler() *Handler {
	return &Handler{
		startTime: time.Now(),
		checkers:  make(map[string]Checker),
	}
}

// AddChecker registers a named readiness dependency checker.
func (h *Handler) AddChecker(name string, checker Checker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checkers[name] = checker
}

// Healthz handles liveness checks. Always returns 200 if the process is running.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"status":         "ok",
		"uptime_seconds": time.Since(h.startTime).Seconds(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// Readyz handles readiness checks, executing all registered checkers with a short timeout.
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	h.mu.RLock()
	checkers := make(map[string]Checker, len(h.checkers))
	for k, v := range h.checkers {
		checkers[k] = v
	}
	h.mu.RUnlock()

	failures := make(map[string]string)
	for name, checker := range checkers {
		if err := checker(ctx); err != nil {
			failures[name] = err.Error()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if len(failures) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":   "unavailable",
			"failures": failures,
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ready",
	})
}
