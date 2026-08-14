package parser

import (
	"testing"
)

func TestTextParser(t *testing.T) {
	tags := map[string]string{"env": "prod"}
	p := NewTextParser("node-01", "syslog", tags)

	tests := []struct {
		name      string
		line      string
		wantLevel string
		wantMsg   string
		wantErr   bool
	}{
		{
			name:      "simple info line",
			line:      "2026-08-14 12:00:00 [INFO] service started successfully",
			wantLevel: "INFO",
			wantMsg:   "2026-08-14 12:00:00 [INFO] service started successfully",
			wantErr:   false,
		},
		{
			name:      "warning line",
			line:      "database disk usage at 85% WARNING",
			wantLevel: "WARN",
			wantMsg:   "database disk usage at 85% WARNING",
			wantErr:   false,
		},
		{
			name:      "error line",
			line:      "FATAL: failed to bind port 8080",
			wantLevel: "FATAL",
			wantMsg:   "FATAL: failed to bind port 8080",
			wantErr:   false,
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := p.Parse(tt.line)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if entry.Level != tt.wantLevel {
					t.Errorf("Level = %q, want %q", entry.Level, tt.wantLevel)
				}
				if entry.Message != tt.wantMsg {
					t.Errorf("Message = %q, want %q", entry.Message, tt.wantMsg)
				}
				if entry.Host != "node-01" || entry.Source != "syslog" {
					t.Errorf("Host/Source mismatch: %s / %s", entry.Host, entry.Source)
				}
				if entry.Fields["env"] != "prod" {
					t.Errorf("Tags mismatch: %v", entry.Fields)
				}
			}
		})
	}
}

func TestJSONParser(t *testing.T) {
	p := NewJSONParser("node-01", "api", nil)

	tests := []struct {
		name      string
		line      string
		wantLevel string
		wantMsg   string
		checkTime bool
		wantErr   bool
	}{
		{
			name:      "standard JSON log",
			line:      `{"timestamp":"2026-08-14T12:00:00Z","level":"ERROR","message":"database timeout","query":"SELECT 1"}`,
			wantLevel: "ERROR",
			wantMsg:   "database timeout",
			checkTime: true,
			wantErr:   false,
		},
		{
			name:      "alternative field names (ts, severity, msg)",
			line:      `{"ts":1770000000,"severity":"WARN","msg":"high latency"}`,
			wantLevel: "WARN",
			wantMsg:   "high latency",
			wantErr:   false,
		},
		{
			name:      "host and source override in json",
			line:      `{"host":"custom-host","source":"custom-app","message":"event occurred"}`,
			wantLevel: "INFO",
			wantMsg:   "event occurred",
			wantErr:   false,
		},
		{
			name:    "invalid json string",
			line:    `{not valid json`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := p.Parse(tt.line)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if entry.Level != tt.wantLevel {
					t.Errorf("Level = %q, want %q", entry.Level, tt.wantLevel)
				}
				if entry.Message != tt.wantMsg {
					t.Errorf("Message = %q, want %q", entry.Message, tt.wantMsg)
				}
				if tt.checkTime && entry.Timestamp.Year() != 2026 {
					t.Errorf("Timestamp year = %d, want 2026", entry.Timestamp.Year())
				}
			}
		})
	}
}

func TestRegexpParser(t *testing.T) {
	pattern := `^(?P<timestamp>\S+) \[(?P<level>\S+)\] (?P<message>.*)$`
	p, err := NewRegexpParser(pattern, "node-01", "nginx", nil)
	if err != nil {
		t.Fatalf("NewRegexpParser failed: %v", err)
	}

	line := "2026-08-14T12:00:00Z [ERROR] failed to reach backend"
	entry, err := p.Parse(line)
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	if entry.Level != "ERROR" {
		t.Errorf("expected level ERROR, got %s", entry.Level)
	}
	if entry.Message != "failed to reach backend" {
		t.Errorf("expected message 'failed to reach backend', got %s", entry.Message)
	}
	if entry.Timestamp.Year() != 2026 {
		t.Errorf("expected year 2026, got %d", entry.Timestamp.Year())
	}
}

func TestAutoParser(t *testing.T) {
	p := NewAutoParser("node-01", "app", nil)

	// JSON line
	entry1, err := p.Parse(`{"level":"WARN","message":"json log message"}`)
	if err != nil {
		t.Fatalf("Parse json failed: %v", err)
	}
	if entry1.Level != "WARN" || entry1.Message != "json log message" {
		t.Errorf("AutoParser JSON mismatch: %+v", entry1)
	}

	// Plain text line
	entry2, err := p.Parse("ERROR connection refused")
	if err != nil {
		t.Fatalf("Parse text failed: %v", err)
	}
	if entry2.Level != "ERROR" || entry2.Message != "ERROR connection refused" {
		t.Errorf("AutoParser Text mismatch: %+v", entry2)
	}
}

func FuzzJSONParser(f *testing.F) {
	p := NewJSONParser("node-01", "app", nil)
	f.Add(`{"timestamp":"2026-08-14T12:00:00Z","level":"ERROR","message":"test"}`)
	f.Add(`{"ts":1770000000,"severity":"INFO","msg":"another test"}`)
	f.Add(`malformed json`)
	f.Add(`{}`)
	f.Add(`{"level": null, "message": 12345}`)

	f.Fuzz(func(t *testing.T, input string) {
		// Must never panic
		_, _ = p.Parse(input)
	})
}

func FuzzRegexpParser(f *testing.F) {
	pattern := `^(?P<timestamp>\S+) \[(?P<level>\S+)\] (?P<message>.*)$`
	p, err := NewRegexpParser(pattern, "node-01", "app", nil)
	if err != nil {
		f.Fatalf("NewRegexpParser: %v", err)
	}

	f.Add("2026-08-14T12:00:00Z [INFO] all good")
	f.Add("random line that doesn't match")
	f.Add("")

	f.Fuzz(func(t *testing.T, input string) {
		// Must never panic
		_, _ = p.Parse(input)
	})
}

func FuzzAutoParser(f *testing.F) {
	p := NewAutoParser("node-01", "app", nil)
	f.Add(`{"level":"ERROR","message":"json log"}`)
	f.Add("2026-08-14 plain text error log")
	f.Add(`{"bad: json`)
	f.Add("")

	f.Fuzz(func(t *testing.T, input string) {
		// Must never panic
		_, _ = p.Parse(input)
	})
}

func BenchmarkTextParser(b *testing.B) {
	p := NewTextParser("node-01", "syslog", nil)
	line := "2026-08-14 12:00:00 [ERROR] failed to connect to database"

	b.ResetTimer()
	for b.Loop() {
		_, _ = p.Parse(line)
	}
}

func BenchmarkJSONParser(b *testing.B) {
	p := NewJSONParser("node-01", "api", nil)
	line := `{"timestamp":"2026-08-14T12:00:00Z","level":"ERROR","message":"database timeout","query":"SELECT 1"}`

	b.ResetTimer()
	for b.Loop() {
		_, _ = p.Parse(line)
	}
}

func BenchmarkRegexpParser(b *testing.B) {
	pattern := `^(?P<timestamp>\S+) \[(?P<level>\S+)\] (?P<message>.*)$`
	p, _ := NewRegexpParser(pattern, "node-01", "nginx", nil)
	line := "2026-08-14T12:00:00Z [ERROR] failed to reach backend"

	b.ResetTimer()
	for b.Loop() {
		_, _ = p.Parse(line)
	}
}
