package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAgentMetrics(t *testing.T) {
	m := NewAgentMetrics()
	m.EventsReadTotal.WithLabelValues("nginx").Inc()
	m.EventsParsedTotal.WithLabelValues("nginx").Inc()
	m.ParseErrorsTotal.WithLabelValues("nginx").Inc()
	m.EventsSentTotal.Add(5)
	m.BatchesSentTotal.Inc()
	m.SpoolBytes.Set(1024)
	m.SpoolEvents.Set(10)
	m.SourcesActive.Set(2)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from metrics handler, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "barnacles_agent_events_read_total") {
		t.Errorf("metrics body missing agent metrics: %s", body)
	}
}

func TestServerMetrics(t *testing.T) {
	m := NewServerMetrics()
	m.EventsIngestedTotal.Add(10)
	m.EventsStoredTotal.Add(10)
	m.WebsocketClients.Set(3)
	m.WebsocketDisconnectsTotal.WithLabelValues("buffer_overflow").Inc()
	m.EventsBroadcastTotal.Add(10)
	m.StorageBytes.Set(4096)
	m.QueryDuration.Observe(0.005)
	m.IngestDuration.Observe(0.002)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from metrics handler, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "barnacles_server_events_ingested_total") {
		t.Errorf("metrics body missing server metrics: %s", body)
	}
}
