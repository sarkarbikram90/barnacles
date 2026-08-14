# Barnacles Configuration Reference

Barnacles supports YAML configuration files with environment variable interpolation using `${ENV_VAR:-default}` syntax.

---

## 1. Server Configuration (`server.yaml`)

```yaml
server:
  address: ":8080"                  # HTTP listening address
  read_header_timeout: 5s           # Protection against Slowloris attacks
  read_timeout: 15s                 # Maximum duration reading request
  write_timeout: 15s                # Maximum duration writing response
  idle_timeout: 60s                 # Idle keep-alive connection timeout
  max_body_bytes: 10485760          # Maximum request payload size (10MB)
  web_dir: "./web"                  # Path to static Web UI directory

auth:
  enabled: false                    # Enable token-based authentication
  tokens:
    - ${BARNACLES_AUTH_TOKEN}       # List of authorized Bearer tokens

tls:
  enabled: false                    # Enable HTTPS/TLS
  cert_file: "/etc/barnacles/server.crt"
  key_file: "/etc/barnacles/server.key"

ingest:
  max_batch_events: 500             # Maximum events allowed in single batch
  max_message_bytes: 1048576        # Max individual message size (1MB)
  dedup_window: 5m                  # Window for idempotency deduplication
  dedup_capacity: 50000             # Maximum UUIDs tracked in dedup cache

storage:
  directory: "./data/logs"          # Path for time-segmented log files
  sync_on_write: false              # Call fsync() on every append write

stream:
  recent_events: 10000              # In-memory recent ring buffer capacity
  max_clients: 100                  # Maximum concurrent WebSocket clients
  client_buffer_size: 256           # Per-client outbound channel buffer
  ping_interval: 30s                # WebSocket heartbeat ping interval
  write_deadline: 5s                # WebSocket frame write timeout

retention:
  enabled: true                     # Enable background disk pruning
  max_size_gb: 10                   # Maximum storage size before pruning oldest logs
  max_age_hours: 168                # Maximum log age (7 days)
  check_interval: 10m               # Background retention check period
```

---

## 2. Agent Configuration (`agent.yaml`)

```yaml
agent:
  id: "agent-node-01"               # Unique identifier for the agent
  host: "node-01"                   # Default host metadata tag
  metrics_address: ":9090"          # Prometheus metrics & health address

server:
  url: "http://localhost:8080"      # Central server URL
  token: ${BARNACLES_AUTH_TOKEN}    # Optional Bearer auth token
  timeout: 10s                      # HTTP request timeout
  insecure_skip_verify: false       # Skip TLS verification (testing only)

batch:
  max_events: 500                   # Flush when batch reaches 500 events
  flush_interval: 1s                # Flush at least every 1 second
  max_queue_events: 5000            # In-memory internal channel queue capacity

retry:
  initial_backoff: 1s               # Initial retry backoff
  max_backoff: 30s                  # Maximum backoff ceiling
  backoff_multiplier: 2.0           # Exponential multiplier
  max_retries: 0                    # 0 = infinite retry until server recovers

spool:
  enabled: true                     # Enable disk persistence during outages
  directory: "./data/agent-spool"   # Path to local disk spool directory
  max_size_mb: 1024                 # 1GB maximum spool capacity on disk
  max_batch_events: 500             # Events per spool file segment

sources:
  - name: "app-service"             # Unique source name
    path: "/var/log/app.log"        # File path to tail
    format: "auto"                  # Parser: "auto", "json", "text", "regexp"
    start_position: "end"           # "beginning" (replay) or "end" (live only)
    tags:                           # Static metadata tags attached to all entries
      env: "production"
      tier: "backend"

  - name: "nginx-access"
    path: "/var/log/nginx/access.log"
    format: "json"
    start_position: "end"
    tags:
      env: "production"
      role: "gateway"

  - name: "custom-pattern"
    path: "/var/log/custom.log"
    format: "regexp"
    pattern: '^(?P<timestamp>\S+) \[(?P<level>\S+)\] (?P<message>.*)$'
    start_position: "end"
```
