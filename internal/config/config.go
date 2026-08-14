// Package config provides strongly-typed configuration structures,
// safe defaults, environment variable substitution, and fail-fast validation.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	// ErrMissingServerAddress indicates server address is empty.
	ErrMissingServerAddress = errors.New("server address is required")
	// ErrMissingStorageDir indicates storage directory is empty.
	ErrMissingStorageDir = errors.New("storage directory is required")
	// ErrMissingAgentID indicates agent ID is required.
	ErrMissingAgentID = errors.New("agent ID is required")
	// ErrMissingServerURL indicates agent target server URL is required.
	ErrMissingServerURL = errors.New("server URL is required")
	// ErrInvalidSource indicates a source configuration is invalid.
	ErrInvalidSource = errors.New("invalid log source configuration")
)

// ServerConfig holds the full central server configuration.
type ServerConfig struct {
	Server    ServerSettings    `yaml:"server"`
	Auth      AuthSettings      `yaml:"auth"`
	TLS       TLSSettings       `yaml:"tls"`
	Ingest    IngestSettings    `yaml:"ingest"`
	Storage   StorageSettings   `yaml:"storage"`
	Stream    StreamSettings    `yaml:"stream"`
	Retention RetentionSettings `yaml:"retention"`
}

// ServerSettings configures the HTTP listening options.
type ServerSettings struct {
	Address           string        `yaml:"address"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	MaxBodyBytes      int64         `yaml:"max_body_bytes"`
	WebDir            string        `yaml:"web_dir"`
}

// AuthSettings defines API token authentication parameters.
type AuthSettings struct {
	Enabled bool     `yaml:"enabled"`
	Tokens  []string `yaml:"tokens"`
}

// TLSSettings defines HTTPS/TLS configuration.
type TLSSettings struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// IngestSettings configures batch ingestion rules and idempotency dedup.
type IngestSettings struct {
	MaxBatchEvents  int           `yaml:"max_batch_events"`
	MaxMessageBytes int           `yaml:"max_message_bytes"`
	DedupWindow     time.Duration `yaml:"dedup_window"`
	DedupCapacity   int           `yaml:"dedup_capacity"`
}

// StorageSettings configures filesystem log persistence.
type StorageSettings struct {
	Directory   string `yaml:"directory"`
	SyncOnWrite bool   `yaml:"sync_on_write"`
}

// StreamSettings configures real-time streaming and WebSocket clients.
type StreamSettings struct {
	RecentEvents     int           `yaml:"recent_events"`
	MaxClients       int           `yaml:"max_clients"`
	ClientBufferSize int           `yaml:"client_buffer_size"`
	PingInterval     time.Duration `yaml:"ping_interval"`
	WriteDeadline    time.Duration `yaml:"write_deadline"`
}

// RetentionSettings configures disk storage pruning.
type RetentionSettings struct {
	Enabled       bool          `yaml:"enabled"`
	MaxSizeGB     int           `yaml:"max_size_gb"`
	MaxAgeHours   int           `yaml:"max_age_hours"`
	CheckInterval time.Duration `yaml:"check_interval"`
}

// DefaultServerConfig returns a safe, production-ready ServerConfig with sensible defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Server: ServerSettings{
			Address:           ":8080",
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxBodyBytes:      10 * 1024 * 1024, // 10MB
		},
		Auth: AuthSettings{
			Enabled: false,
			Tokens:  []string{},
		},
		TLS: TLSSettings{
			Enabled: false,
		},
		Ingest: IngestSettings{
			MaxBatchEvents:  500,
			MaxMessageBytes: 1024 * 1024, // 1MB
			DedupWindow:     5 * time.Minute,
			DedupCapacity:   50000,
		},
		Storage: StorageSettings{
			Directory:   "./data/logs",
			SyncOnWrite: false,
		},
		Stream: StreamSettings{
			RecentEvents:     10000,
			MaxClients:       100,
			ClientBufferSize: 256,
			PingInterval:     30 * time.Second,
			WriteDeadline:    5 * time.Second,
		},
		Retention: RetentionSettings{
			Enabled:       true,
			MaxSizeGB:     10,
			MaxAgeHours:   168, // 7 days
			CheckInterval: 10 * time.Minute,
		},
	}
}

// Validate checks ServerConfig for invalid or unsafe values.
func (c *ServerConfig) Validate() error {
	if strings.TrimSpace(c.Server.Address) == "" {
		return ErrMissingServerAddress
	}
	if strings.TrimSpace(c.Storage.Directory) == "" {
		return ErrMissingStorageDir
	}
	if c.Ingest.MaxBatchEvents <= 0 {
		c.Ingest.MaxBatchEvents = 500
	}
	if c.Ingest.MaxMessageBytes <= 0 {
		c.Ingest.MaxMessageBytes = 1024 * 1024
	}
	if c.Ingest.DedupCapacity <= 0 {
		c.Ingest.DedupCapacity = 50000
	}
	if c.Stream.RecentEvents <= 0 {
		c.Stream.RecentEvents = 10000
	}
	if c.Stream.MaxClients <= 0 {
		c.Stream.MaxClients = 100
	}
	if c.Stream.ClientBufferSize <= 0 {
		c.Stream.ClientBufferSize = 256
	}
	if c.Auth.Enabled && len(c.Auth.Tokens) == 0 {
		return errors.New("auth is enabled but no tokens configured")
	}
	if c.TLS.Enabled {
		if c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
			return errors.New("tls enabled requires both cert_file and key_file")
		}
	}
	return nil
}

// AgentConfig defines the configuration for a running Barnacles Agent.
type AgentConfig struct {
	Agent   AgentSettings  `yaml:"agent"`
	Server  ServerTarget   `yaml:"server"`
	Batch   BatchSettings  `yaml:"batch"`
	Retry   RetrySettings  `yaml:"retry"`
	Spool   SpoolSettings  `yaml:"spool"`
	Sources []SourceConfig `yaml:"sources"`
}

// AgentSettings configures agent identification and metrics.
type AgentSettings struct {
	ID             string `yaml:"id"`
	Host           string `yaml:"host"`
	MetricsAddress string `yaml:"metrics_address"`
}

// ServerTarget configures connection to the central Barnacles server.
type ServerTarget struct {
	URL                string        `yaml:"url"`
	Token              string        `yaml:"token"`
	Timeout            time.Duration `yaml:"timeout"`
	InsecureSkipVerify bool          `yaml:"insecure_skip_verify"`
}

// BatchSettings configures batching before delivery.
type BatchSettings struct {
	MaxEvents      int           `yaml:"max_events"`
	FlushInterval  time.Duration `yaml:"flush_interval"`
	MaxQueueEvents int           `yaml:"max_queue_events"`
}

// RetrySettings configures exponential backoff jitter.
type RetrySettings struct {
	InitialBackoff    time.Duration `yaml:"initial_backoff"`
	MaxBackoff        time.Duration `yaml:"max_backoff"`
	BackoffMultiplier float64       `yaml:"backoff_multiplier"`
	MaxRetries        int           `yaml:"max_retries"`
}

// SpoolSettings configures disk-backed persistence during outages.
type SpoolSettings struct {
	Enabled        bool   `yaml:"enabled"`
	Directory      string `yaml:"directory"`
	MaxSizeMB      int    `yaml:"max_size_mb"`
	MaxBatchEvents int    `yaml:"max_batch_events"`
}

// SourceConfig configures a single log file to tail and parse.
type SourceConfig struct {
	Name          string            `yaml:"name"`
	Path          string            `yaml:"path"`
	Format        string            `yaml:"format"` // text, json, regexp, auto
	Pattern       string            `yaml:"pattern"`
	StartPosition string            `yaml:"start_position"` // beginning or end
	Tags          map[string]string `yaml:"tags"`
}

// DefaultAgentConfig returns safe, production-ready Agent defaults.
func DefaultAgentConfig() AgentConfig {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "agent-node"
	}

	return AgentConfig{
		Agent: AgentSettings{
			ID:             hostname,
			Host:           hostname,
			MetricsAddress: ":9090",
		},
		Server: ServerTarget{
			URL:     "http://localhost:8080",
			Timeout: 10 * time.Second,
		},
		Batch: BatchSettings{
			MaxEvents:      500,
			FlushInterval:  1 * time.Second,
			MaxQueueEvents: 5000,
		},
		Retry: RetrySettings{
			InitialBackoff:    1 * time.Second,
			MaxBackoff:        30 * time.Second,
			BackoffMultiplier: 2.0,
			MaxRetries:        0, // infinite retry with backoff for retryable errors
		},
		Spool: SpoolSettings{
			Enabled:        true,
			Directory:      "./data/agent-spool",
			MaxSizeMB:      1024,
			MaxBatchEvents: 500,
		},
		Sources: []SourceConfig{},
	}
}

// Validate checks AgentConfig for valid settings.
func (c *AgentConfig) Validate() error {
	if strings.TrimSpace(c.Agent.ID) == "" {
		return ErrMissingAgentID
	}
	if strings.TrimSpace(c.Agent.Host) == "" {
		c.Agent.Host = c.Agent.ID
	}
	if strings.TrimSpace(c.Server.URL) == "" {
		return ErrMissingServerURL
	}
	if c.Batch.MaxEvents <= 0 {
		c.Batch.MaxEvents = 500
	}
	if c.Batch.FlushInterval <= 0 {
		c.Batch.FlushInterval = 1 * time.Second
	}
	if c.Batch.MaxQueueEvents <= 0 {
		c.Batch.MaxQueueEvents = 5000
	}
	if c.Retry.InitialBackoff <= 0 {
		c.Retry.InitialBackoff = 1 * time.Second
	}
	if c.Retry.MaxBackoff <= 0 {
		c.Retry.MaxBackoff = 30 * time.Second
	}
	if c.Retry.BackoffMultiplier <= 1.0 {
		c.Retry.BackoffMultiplier = 2.0
	}
	if c.Spool.Enabled {
		if strings.TrimSpace(c.Spool.Directory) == "" {
			return errors.New("spool directory is required when spool is enabled")
		}
		if c.Spool.MaxSizeMB <= 0 {
			c.Spool.MaxSizeMB = 1024
		}
	}
	for i, src := range c.Sources {
		if strings.TrimSpace(src.Name) == "" {
			return fmt.Errorf("%w: source index %d has empty name", ErrInvalidSource, i)
		}
		if strings.TrimSpace(src.Path) == "" {
			return fmt.Errorf("%w: source %q has empty path", ErrInvalidSource, src.Name)
		}
		format := strings.ToLower(strings.TrimSpace(src.Format))
		if format == "" {
			format = "auto"
		}
		if format != "auto" && format != "text" && format != "json" && format != "regexp" {
			return fmt.Errorf("%w: source %q has unsupported format %q", ErrInvalidSource, src.Name, src.Format)
		}
		if format == "regexp" && strings.TrimSpace(src.Pattern) == "" {
			return fmt.Errorf("%w: source %q format is regexp but pattern is empty", ErrInvalidSource, src.Name)
		}
		startPos := strings.ToLower(strings.TrimSpace(src.StartPosition))
		if startPos == "" {
			startPos = "end"
		}
		if startPos != "beginning" && startPos != "end" {
			return fmt.Errorf("%w: source %q start_position must be 'beginning' or 'end'", ErrInvalidSource, src.Name)
		}
		c.Sources[i].Format = format
		c.Sources[i].StartPosition = startPos
	}
	return nil
}

var envVarPattern = regexp.MustCompile(`\$\{([a-zA-Z_][a-zA-Z0-9_]*)(?::-([^}]*))?\}`)

// ExpandEnv expands environment variables in a YAML string, supporting syntax:
// ${VAR} and ${VAR:-default}
func ExpandEnv(input string) string {
	return envVarPattern.ReplaceAllStringFunc(input, func(match string) string {
		submatches := envVarPattern.FindStringSubmatch(match)
		if len(submatches) < 2 {
			return match
		}
		varName := submatches[1]
		val, exists := os.LookupEnv(varName)
		if exists && val != "" {
			return val
		}
		if len(submatches) >= 3 && submatches[2] != "" {
			return submatches[2]
		}
		return val
	})
}

// LoadServerConfig reads a YAML file, expands environment variables, applies defaults, and validates.
func LoadServerConfig(filePath string) (ServerConfig, error) {
	cfg := DefaultServerConfig()
	if filePath == "" {
		return cfg, cfg.Validate()
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return cfg, fmt.Errorf("read server config file: %w", err)
	}

	expanded := ExpandEnv(string(data))
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return cfg, fmt.Errorf("parse server config YAML: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("validate server config: %w", err)
	}

	return cfg, nil
}

// LoadAgentConfig reads a YAML file, expands environment variables, applies defaults, and validates.
func LoadAgentConfig(filePath string) (AgentConfig, error) {
	cfg := DefaultAgentConfig()
	if filePath == "" {
		return cfg, cfg.Validate()
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return cfg, fmt.Errorf("read agent config file: %w", err)
	}

	expanded := ExpandEnv(string(data))
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return cfg, fmt.Errorf("parse agent config YAML: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("validate agent config: %w", err)
	}

	return cfg, nil
}
