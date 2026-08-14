package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/store"
	"github.com/sarkarbikram90/barnacles/internal/stream"
)

// handleLogsQuery processes GET /api/v1/logs.
func handleLogsQuery(st store.LogStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		q := logentry.Query{
			Host:   r.URL.Query().Get("host"),
			Source: r.URL.Query().Get("source"),
			Level:  r.URL.Query().Get("level"),
			Search: r.URL.Query().Get("search"),
		}

		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil {
				q.Limit = l
			}
		}

		if startStr := r.URL.Query().Get("start_time"); startStr != "" {
			if t, err := time.Parse(time.RFC3339, startStr); err == nil {
				q.StartTime = t
			}
		}

		if endStr := r.URL.Query().Get("end_time"); endStr != "" {
			if t, err := time.Parse(time.RFC3339, endStr); err == nil {
				q.EndTime = t
			}
		}

		entries, err := st.Query(r.Context(), q)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": len(entries),
			"logs":  entries,
		})
	}
}

// handleSourcesQuery processes GET /api/v1/sources.
func handleSourcesQuery(st store.LogStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sources, err := st.Sources(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sources": sources,
		})
	}
}

// handleHostsQuery processes GET /api/v1/agents or GET /api/v1/hosts.
func handleHostsQuery(st store.LogStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hosts, err := st.Hosts(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"agents": hosts,
			"hosts":  hosts,
		})
	}
}

// handleWebSocketStream processes GET /ws and GET /api/v1/stream.
func handleWebSocketStream(hub *stream.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := hub.Upgrade(w, r); err != nil {
			// Upgrade handles error responses
			return
		}
	}
}
