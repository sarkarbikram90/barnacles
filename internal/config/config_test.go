package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExpandEnv(t *testing.T) {
	os.Setenv("TEST_VAR_A", "production_server")
	defer os.Unsetenv("TEST_VAR_A")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "existing env var",
			input:    "address: ${TEST_VAR_A}",
			expected: "address: production_server",
		},
		{
			name:     "missing with default",
			input:    "port: ${NON_EXISTENT_VAR:-8080}",
			expected: "port: 8080",
		},
		{
			name:     "missing without default",
			input:    "token: ${MISSING_VAR}",
			expected: "token: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExpandEnv(tt.input)
			if got != tt.expected {
				t.Errorf("ExpandEnv(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestServerConfigLoadAndValidate(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "server.yaml")

	content := `
server:
  address: ":9999"
  read_timeout: 20s
ingest:
  max_batch_events: 1000
storage:
  directory: "/tmp/barnacles/data"
`
	if err := os.WriteFile(configFile, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg, err := LoadServerConfig(configFile)
	if err != nil {
		t.Fatalf("LoadServerConfig() failed: %v", err)
	}

	if cfg.Server.Address != ":9999" {
		t.Errorf("expected address :9999, got %s", cfg.Server.Address)
	}
	if cfg.Server.ReadTimeout != 20*time.Second {
		t.Errorf("expected ReadTimeout 20s, got %v", cfg.Server.ReadTimeout)
	}
	if cfg.Ingest.MaxBatchEvents != 1000 {
		t.Errorf("expected MaxBatchEvents 1000, got %d", cfg.Ingest.MaxBatchEvents)
	}
	if cfg.Storage.Directory != "/tmp/barnacles/data" {
		t.Errorf("expected Directory /tmp/barnacles/data, got %s", cfg.Storage.Directory)
	}
}

func TestAgentConfigLoadAndValidate(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "agent.yaml")

	content := `
agent:
  id: "worker-node-1"
server:
  url: "http://central:8080"
sources:
  - name: "app-logs"
    path: "/var/log/app.log"
    format: "json"
    start_position: "beginning"
`
	if err := os.WriteFile(configFile, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg, err := LoadAgentConfig(configFile)
	if err != nil {
		t.Fatalf("LoadAgentConfig() failed: %v", err)
	}

	if cfg.Agent.ID != "worker-node-1" {
		t.Errorf("expected ID worker-node-1, got %s", cfg.Agent.ID)
	}
	if cfg.Server.URL != "http://central:8080" {
		t.Errorf("expected URL http://central:8080, got %s", cfg.Server.URL)
	}
	if len(cfg.Sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(cfg.Sources))
	}
	src := cfg.Sources[0]
	if src.Name != "app-logs" || src.Format != "json" || src.StartPosition != "beginning" {
		t.Errorf("unexpected source config: %+v", src)
	}
}

func TestAgentConfigValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*AgentConfig)
		wantErr bool
	}{
		{
			name: "empty agent id",
			mutate: func(c *AgentConfig) {
				c.Agent.ID = ""
			},
			wantErr: true,
		},
		{
			name: "empty server url",
			mutate: func(c *AgentConfig) {
				c.Server.URL = ""
			},
			wantErr: true,
		},
		{
			name: "invalid source format",
			mutate: func(c *AgentConfig) {
				c.Sources = []SourceConfig{
					{Name: "s1", Path: "/tmp/a.log", Format: "invalid_format"},
				}
			},
			wantErr: true,
		},
		{
			name: "regexp source missing pattern",
			mutate: func(c *AgentConfig) {
				c.Sources = []SourceConfig{
					{Name: "s1", Path: "/tmp/a.log", Format: "regexp", Pattern: ""},
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultAgentConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
