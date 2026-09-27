package ingest

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
	"github.com/sarkarbikram90/barnacles/internal/store"
)

func TestParseOTLPLogsJSON(t *testing.T) {
	otlpJSON := `{
		"resourceLogs": [
			{
				"resource": {
					"attributes": [
						{ "key": "service.name", "value": { "stringValue": "payment-gateway" } },
						{ "key": "host.name", "value": { "stringValue": "edge-k8s-01" } },
						{ "key": "region", "value": { "stringValue": "us-east" } }
					]
				},
				"scopeLogs": [
					{
						"scope": { "name": "stripe.client" },
						"logRecords": [
							{
								"timeUnixNano": "1727457367000000000",
								"severityNumber": 17,
								"severityText": "ERROR",
								"body": { "stringValue": "charge failed: card declined" },
								"attributes": [
									{ "key": "card.brand", "value": { "stringValue": "visa" } },
									{ "key": "retry_count", "value": { "intValue": 2 } }
								],
								"traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
								"spanId": "00f067aa0ba902b7"
							}
						]
					}
				]
			}
		]
	}`

	entries, err := ParseOTLPLogsJSON(strings.NewReader(otlpJSON))
	if err != nil {
		t.Fatalf("ParseOTLPLogsJSON failed: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Host != "edge-k8s-01" {
		t.Errorf("expected host 'edge-k8s-01', got %q", entry.Host)
	}
	if entry.Source != "stripe.client" {
		t.Errorf("expected source 'stripe.client', got %q", entry.Source)
	}
	if entry.Level != "ERROR" {
		t.Errorf("expected level 'ERROR', got %q", entry.Level)
	}
	if entry.Message != "charge failed: card declined" {
		t.Errorf("expected message 'charge failed: card declined', got %q", entry.Message)
	}
	if entry.Fields["card.brand"] != "visa" {
		t.Errorf("expected field card.brand='visa', got %q", entry.Fields["card.brand"])
	}
	if entry.Fields["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("expected trace_id, got %q", entry.Fields["trace_id"])
	}
	if entry.Fields["span_id"] != "00f067aa0ba902b7" {
		t.Errorf("expected span_id, got %q", entry.Fields["span_id"])
	}
}

func TestServeOTLPHTTP(t *testing.T) {
	tempDir := t.TempDir()
	fsStore, err := store.NewFileStore(store.Config{Directory: filepath.Join(tempDir, "logs")})
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}
	defer fsStore.Close()

	handler := NewHandler(config.IngestSettings{
		MaxBatchEvents: 100,
		DedupWindow:    5 * time.Minute,
		DedupCapacity:  1000,
	}, fsStore, nil, metrics.NewServerMetrics())

	otlpPayload := `{
		"resourceLogs": [
			{
				"resource": {
					"attributes": [
						{ "key": "service.name", "value": { "stringValue": "auth-service" } }
					]
				},
				"scopeLogs": [
					{
						"logRecords": [
							{
								"severityText": "INFO",
								"body": { "stringValue": "user logged in successfully" }
							}
						]
					}
				]
			}
		]
	}`

	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(otlpPayload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeOTLP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify entry was written to store
	stored, err := fsStore.Query(context.Background(), logentry.Query{Limit: 10})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("expected 1 stored entry, got %d", len(stored))
	}
	if stored[0].Message != "user logged in successfully" {
		t.Errorf("unexpected stored message: %s", stored[0].Message)
	}
	if stored[0].Source != "auth-service" {
		t.Errorf("unexpected stored source: %s", stored[0].Source)
	}
}
