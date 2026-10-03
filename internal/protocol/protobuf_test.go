package protocol_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/protocol"
)

func sampleEntries(n int) []logentry.LogEntry {
	now := time.Now().UTC().Truncate(time.Microsecond)
	entries := make([]logentry.LogEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = logentry.LogEntry{
			ID:        "550e8400-e29b-41d4-a716-446655440000",
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			Host:      "prod-app-server-01.us-east.internal",
			Source:    "order-service",
			Level:     "INFO",
			Message:   "Processed payment transaction for customer #98124 via stripe gateway successfully",
			Fields: map[string]string{
				"trace_id":   "4bf92f3577b34da6a3ce929d0e0e4736",
				"span_id":    "00f067aa0ba902b7",
				"user_id":    "usr_8237194",
				"env":        "production",
				"datacenter": "us-east-1a",
			},
		}
	}
	return entries
}

func TestProtobufLogEntry_RoundTrip(t *testing.T) {
	original := sampleEntries(1)[0]
	b := protocol.MarshalLogEntry(&original)
	if len(b) == 0 {
		t.Fatal("expected non-empty binary payload")
	}

	var decoded logentry.LogEntry
	if err := protocol.UnmarshalLogEntry(b, &decoded); err != nil {
		t.Fatalf("failed to unmarshal log entry: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID mismatch: got %q, want %q", decoded.ID, original.ID)
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp mismatch: got %v, want %v", decoded.Timestamp, original.Timestamp)
	}
	if decoded.Host != original.Host {
		t.Errorf("Host mismatch: got %q, want %q", decoded.Host, original.Host)
	}
	if decoded.Source != original.Source {
		t.Errorf("Source mismatch: got %q, want %q", decoded.Source, original.Source)
	}
	if decoded.Level != original.Level {
		t.Errorf("Level mismatch: got %q, want %q", decoded.Level, original.Level)
	}
	if decoded.Message != original.Message {
		t.Errorf("Message mismatch: got %q, want %q", decoded.Message, original.Message)
	}
	if len(decoded.Fields) != len(original.Fields) {
		t.Fatalf("Fields length mismatch: got %d, want %d", len(decoded.Fields), len(original.Fields))
	}
	for k, v := range original.Fields {
		if decoded.Fields[k] != v {
			t.Errorf("Field %q mismatch: got %q, want %q", k, decoded.Fields[k], v)
		}
	}
}

func TestProtobufIngestRequest_RoundTrip(t *testing.T) {
	entries := sampleEntries(10)
	req := logentry.IngestRequest{
		AgentID: "agent-ny-01",
		Events:  entries,
	}

	b, err := protocol.MarshalIngestRequest(&req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded logentry.IngestRequest
	if err := protocol.UnmarshalIngestRequest(b, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.AgentID != req.AgentID {
		t.Errorf("AgentID mismatch: got %q, want %q", decoded.AgentID, req.AgentID)
	}
	if len(decoded.Events) != len(req.Events) {
		t.Fatalf("Events count mismatch: got %d, want %d", len(decoded.Events), len(req.Events))
	}

	for i := range decoded.Events {
		if decoded.Events[i].ID != req.Events[i].ID {
			t.Errorf("[%d] ID mismatch", i)
		}
		if decoded.Events[i].Message != req.Events[i].Message {
			t.Errorf("[%d] Message mismatch", i)
		}
	}
}

func TestProtobufIngestResponse_RoundTrip(t *testing.T) {
	resp := logentry.IngestResponse{
		Status:     "ok",
		Accepted:   250,
		Duplicates: 2,
		Errors:     []string{"warning: field length exceeded", "warning: deprecated field"},
	}

	b, err := protocol.MarshalIngestResponse(&resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded logentry.IngestResponse
	if err := protocol.UnmarshalIngestResponse(b, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.Status != resp.Status {
		t.Errorf("Status mismatch: got %q, want %q", decoded.Status, resp.Status)
	}
	if decoded.Accepted != resp.Accepted {
		t.Errorf("Accepted mismatch: got %d, want %d", decoded.Accepted, resp.Accepted)
	}
	if decoded.Duplicates != resp.Duplicates {
		t.Errorf("Duplicates mismatch: got %d, want %d", decoded.Duplicates, resp.Duplicates)
	}
	if len(decoded.Errors) != len(resp.Errors) {
		t.Fatalf("Errors count mismatch: got %d, want %d", len(decoded.Errors), len(resp.Errors))
	}
	for i, e := range resp.Errors {
		if decoded.Errors[i] != e {
			t.Errorf("[%d] Error mismatch: got %q, want %q", i, decoded.Errors[i], e)
		}
	}
}

func BenchmarkProtobufMarshal_100Events(b *testing.B) {
	entries := sampleEntries(100)
	req := logentry.IngestRequest{
		AgentID: "agent-01",
		Events:  entries,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := protocol.MarshalIngestRequest(&req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONMarshal_100Events(b *testing.B) {
	entries := sampleEntries(100)
	req := logentry.IngestRequest{
		AgentID: "agent-01",
		Events:  entries,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(&req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtobufUnmarshal_100Events(b *testing.B) {
	entries := sampleEntries(100)
	req := logentry.IngestRequest{
		AgentID: "agent-01",
		Events:  entries,
	}
	data, _ := protocol.MarshalIngestRequest(&req)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var decoded logentry.IngestRequest
		if err := protocol.UnmarshalIngestRequest(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONUnmarshal_100Events(b *testing.B) {
	entries := sampleEntries(100)
	req := logentry.IngestRequest{
		AgentID: "agent-01",
		Events:  entries,
	}
	data, _ := json.Marshal(&req)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var decoded logentry.IngestRequest
		if err := json.Unmarshal(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}
