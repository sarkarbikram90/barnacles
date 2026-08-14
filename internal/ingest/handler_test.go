package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
	"github.com/sarkarbikram90/barnacles/internal/store"
	"github.com/sarkarbikram90/barnacles/internal/stream"
)

func TestIngestHandlerSuccessAndDedup(t *testing.T) {
	tempDir := t.TempDir()
	st, err := store.NewFileStore(store.Config{Directory: tempDir})
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}
	defer st.Close()

	m := metrics.NewServerMetrics()
	hub := stream.NewHub(config.StreamSettings{RecentEvents: 100}, nil, m)
	defer hub.Close()

	cfg := config.IngestSettings{
		MaxBatchEvents:  100,
		MaxMessageBytes: 1024 * 1024,
		DedupWindow:     time.Minute,
		DedupCapacity:   1000,
	}

	handler := NewHandler(cfg, st, hub, m)

	event := logentry.LogEntry{
		ID:        "evt-001",
		Timestamp: time.Now().UTC(),
		Host:      "srv-01",
		Source:    "api",
		Level:     "INFO",
		Message:   "user logged in",
	}

	reqPayload := logentry.IngestRequest{
		AgentID: "agent-1",
		Events:  []logentry.LogEntry{event},
	}
	body, _ := json.Marshal(reqPayload)

	// First ingest
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewReader(body))
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("first ingest failed, status: %d, body: %s", rec1.Code, rec1.Body.String())
	}

	var resp1 logentry.IngestResponse
	_ = json.NewDecoder(rec1.Body).Decode(&resp1)
	if resp1.Accepted != 1 || resp1.Duplicates != 0 {
		t.Errorf("unexpected resp 1: %+v", resp1)
	}

	// Verify in store
	stored, err := st.Query(context.Background(), logentry.Query{Host: "srv-01"})
	if err != nil || len(stored) != 1 {
		t.Fatalf("store query failed: %v, count=%d", err, len(stored))
	}

	// Second ingest with same event ID (retry / duplicate)
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewReader(body))
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("second ingest failed, status: %d", rec2.Code)
	}

	var resp2 logentry.IngestResponse
	_ = json.NewDecoder(rec2.Body).Decode(&resp2)
	if resp2.Accepted != 0 || resp2.Duplicates != 1 {
		t.Errorf("expected duplicate detected: %+v", resp2)
	}

	// Verify store still has exactly 1 entry (not duplicated)
	stored2, _ := st.Query(context.Background(), logentry.Query{Host: "srv-01"})
	if len(stored2) != 1 {
		t.Fatalf("expected 1 entry in store, got %d", len(stored2))
	}
}

func TestIngestValidationFailures(t *testing.T) {
	tempDir := t.TempDir()
	st, _ := store.NewFileStore(store.Config{Directory: tempDir})
	defer st.Close()

	cfg := config.IngestSettings{
		MaxBatchEvents:  2,
		MaxMessageBytes: 10, // 10 bytes max
	}
	handler := NewHandler(cfg, st, nil, nil)

	// Case 1: Message too large
	eventTooLarge := logentry.LogEntry{
		ID:      "e1",
		Host:    "h",
		Source:  "s",
		Message: "this is way too long",
	}
	reqPayload := logentry.IngestRequest{
		AgentID: "a",
		Events:  []logentry.LogEntry{eventTooLarge},
	}
	body, _ := json.Marshal(reqPayload)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewReader(body))
	handler.ServeHTTP(rec, req)

	var resp logentry.IngestResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Accepted != 0 || len(resp.Errors) == 0 {
		t.Errorf("expected validation error for large message: %+v", resp)
	}

	// Case 2: Batch exceeds max_batch_events
	reqExcess := logentry.IngestRequest{
		AgentID: "a",
		Events: []logentry.LogEntry{
			{ID: "1", Host: "h", Source: "s", Message: "1"},
			{ID: "2", Host: "h", Source: "s", Message: "2"},
			{ID: "3", Host: "h", Source: "s", Message: "3"},
		},
	}
	bodyExcess, _ := json.Marshal(reqExcess)
	recExcess := httptest.NewRecorder()
	reqEx := httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewReader(bodyExcess))
	handler.ServeHTTP(recExcess, reqEx)

	if recExcess.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for batch exceeding max events, got %d", recExcess.Code)
	}
}

func TestDedupCacheCapacity(t *testing.T) {
	cache := NewDedupCache(time.Minute, 5)

	for i := 0; i < 5; i++ {
		if cache.IsDuplicate(string(rune('A' + i))) {
			t.Errorf("expected not duplicate for new key")
		}
	}

	if cache.Len() != 5 {
		t.Errorf("expected cache len 5, got %d", cache.Len())
	}

	// Adding 6th should trigger eviction and not panic
	cache.IsDuplicate("Z")
}
