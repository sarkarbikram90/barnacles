package logentry

import (
	"testing"
	"time"
)

func TestLogEntryNew(t *testing.T) {
	fields := map[string]string{"env": "prod", "cluster": "us-east-1"}
	entry := New("node-1", "syslog", "error", "something broke", fields)

	if entry.ID == "" {
		t.Errorf("expected non-empty ID")
	}
	if entry.Timestamp.IsZero() {
		t.Errorf("expected non-zero Timestamp")
	}
	if entry.Host != "node-1" {
		t.Errorf("expected Host node-1, got %s", entry.Host)
	}
	if entry.Source != "syslog" {
		t.Errorf("expected Source syslog, got %s", entry.Source)
	}
	if entry.Level != "ERROR" {
		t.Errorf("expected Level ERROR, got %s", entry.Level)
	}
	if entry.Message != "something broke" {
		t.Errorf("expected Message 'something broke', got %s", entry.Message)
	}
	if len(entry.Fields) != 2 || entry.Fields["env"] != "prod" {
		t.Errorf("fields not copied correctly: %v", entry.Fields)
	}
}

func TestLogEntryValidate(t *testing.T) {
	tests := []struct {
		name        string
		entry       LogEntry
		maxMsgBytes int
		wantErr     bool
	}{
		{
			name: "valid entry",
			entry: LogEntry{
				ID:        "id-123",
				Timestamp: time.Now(),
				Host:      "host-a",
				Source:    "app",
				Message:   "hello",
			},
			wantErr: false,
		},
		{
			name: "missing host",
			entry: LogEntry{
				Source:  "app",
				Message: "hello",
			},
			wantErr: true,
		},
		{
			name: "missing source",
			entry: LogEntry{
				Host:    "host-a",
				Message: "hello",
			},
			wantErr: true,
		},
		{
			name: "missing message",
			entry: LogEntry{
				Host:   "host-a",
				Source: "app",
			},
			wantErr: true,
		},
		{
			name: "message exceeds limit",
			entry: LogEntry{
				Host:    "host-a",
				Source:  "app",
				Message: "1234567890",
			},
			maxMsgBytes: 5,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate(tt.maxMsgBytes)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogEntryMatches(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	entry := LogEntry{
		ID:        "1",
		Timestamp: now,
		Host:      "srv-01",
		Source:    "nginx",
		Level:     "WARN",
		Message:   "connection timeout to upstream",
		Fields:    map[string]string{"req_id": "xyz-999"},
	}

	tests := []struct {
		name  string
		query Query
		want  bool
	}{
		{
			name:  "match all empty query",
			query: Query{},
			want:  true,
		},
		{
			name:  "match host",
			query: Query{Host: "srv-01"},
			want:  true,
		},
		{
			name:  "mismatch host",
			query: Query{Host: "srv-02"},
			want:  false,
		},
		{
			name:  "match level case-insensitive",
			query: Query{Level: "warn"},
			want:  true,
		},
		{
			name:  "mismatch level",
			query: Query{Level: "ERROR"},
			want:  false,
		},
		{
			name:  "match time range",
			query: Query{StartTime: now.Add(-time.Hour), EndTime: now.Add(time.Hour)},
			want:  true,
		},
		{
			name:  "mismatch before start time",
			query: Query{StartTime: now.Add(time.Minute)},
			want:  false,
		},
		{
			name:  "match search in message",
			query: Query{Search: "upstream"},
			want:  true,
		},
		{
			name:  "match search in fields",
			query: Query{Search: "xyz-999"},
			want:  true,
		},
		{
			name:  "mismatch search",
			query: Query{Search: "database"},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := entry.Matches(tt.query); got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}
