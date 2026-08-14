# Barnacles — Production-Grade MVP Implementation Prompt

You are a senior/staff-level Go engineer and distributed-systems engineer.

Build **Barnacles**, a production-grade MVP distributed log aggregation and real-time log streaming system in Go.

The objective is not to create a toy log tailing application. Build a small but credible production system that demonstrates strong Go engineering, concurrency, reliability, observability, distributed-systems fundamentals, clean architecture, operational maturity, and excellent code quality.

The project should be suitable as a serious open-source portfolio project and should demonstrate the engineering practices expected from a senior/staff-level Go engineer.

---

## 1. Product Definition

Barnacles is a lightweight distributed log aggregation platform.

It consists of:

1. **Barnacles Agent**

   * Runs on individual servers.
   * Watches one or more log files.
   * Tails new log lines.
   * Handles file rotation.
   * Parses and normalizes log entries.
   * Buffers logs locally when the central server is unavailable.
   * Batches logs and sends them to the Barnacles server.
   * Retries failed delivery with exponential backoff.
   * Exposes local health and metrics endpoints.

2. **Barnacles Server**

   * Receives logs from multiple agents.
   * Validates and normalizes incoming events.
   * Performs lightweight routing/filtering.
   * Maintains a bounded in-memory recent-log buffer.
   * Persists received logs to local storage.
   * Publishes logs to connected WebSocket clients.
   * Exposes REST APIs.
   * Exposes health and Prometheus metrics.

3. **Barnacles Web UI**

   * Displays real-time logs.
   * Supports filtering.
   * Shows source/host/level/timestamp.
   * Automatically streams new log events using WebSockets.
   * Handles reconnects.
   * Provides a basic operational dashboard.

The architecture must remain simple enough for an MVP but must not make design decisions that would prevent future scaling.

---

# 2. Primary Engineering Goals

Optimize for the following:

* correctness
* reliability
* simplicity
* observability
* testability
* maintainability
* graceful failure
* predictable resource usage
* clear package boundaries
* idiomatic Go
* operational usability

Do NOT optimize for feature count.

Do NOT introduce distributed systems complexity merely to make the architecture look impressive.

The MVP should be something one engineer can understand and operate.

Prefer a boring, explicit design over a clever design.

---

# 3. Go Version

Target:

```text
Go 1.26+
```

Use modern Go features where they materially improve correctness or readability, but do not introduce language tricks merely because they are available.

Keep dependencies minimal.

Prefer the standard library wherever practical.

---

# 4. Required Engineering Style

The entire implementation MUST follow:

Uber Go Style Guide:

https://github.com/uber-go/guide/blob/master/style.md

Effective Go:

https://go.dev/doc/effective_go

Treat those documents as engineering constraints, not optional recommendations.

In addition to normal `gofmt`, enforce the following principles.

## 4.1 Interfaces

Define interfaces at the point of use.

Do not create interfaces simply because “good architecture requires interfaces.”

Interfaces should exist where they provide:

* dependency inversion
* testability
* multiple implementations
* clear abstraction boundaries

Prefer small interfaces.

For example:

```go
type LogStore interface {
    Append(context.Context, []LogEntry) error
}
```

Avoid large interfaces such as:

```go
type Everything interface {
    Start()
    Stop()
    Store()
    Parse()
    Send()
    Metrics()
    ...
}
```

---

## 4.2 Interface Compliance

Where appropriate, explicitly verify interface implementation:

```go
var _ LogStore = (*FileStore)(nil)
```

Use compile-time interface assertions when they protect an important contract.

---

## 4.3 Error Handling

Follow idiomatic Go error handling.

Errors must:

* be handled explicitly
* be wrapped with context where useful
* use `errors.Is` / `errors.As` when appropriate
* avoid unnecessary custom error types
* avoid logging the same error at every layer

Handle an error once at the appropriate boundary.

Prefer:

```go
return fmt.Errorf("read log file: %w", err)
```

over:

```go
log.Error(err)
return err
```

unless the current layer is actually responsible for reporting the error.

Do not use panic for normal operational failures.

---

## 4.4 Goroutines

Every goroutine must have a clear ownership model.

Do not create unmanaged goroutines.

Every goroutine must have an explicit shutdown mechanism.

Prefer:

```go
go func() {
    if err := worker.Run(ctx); err != nil {
        ...
    }
}()
```

and ensure the parent component controls its lifetime.

Do not use fire-and-forget goroutines.

Use `context.Context` for cancellation and lifecycle control.

---

## 4.5 Channels

Do not use arbitrarily large buffered channels.

Follow Uber's guidance around channel sizing.

Prefer unbuffered or size-one channels unless there is a demonstrated reason for larger buffering.

If a larger buffer is required:

* document why
* define its capacity explicitly
* define behavior when full
* test the backpressure behavior

Never allow an unbounded channel or queue.

---

## 4.6 Mutexes

Use zero-value mutexes where practical.

Prefer:

```go
type Buffer struct {
    mu    sync.Mutex
    items []LogEntry
}
```

over:

```go
type Buffer struct {
    mu    *sync.Mutex
    items []LogEntry
}
```

Do not embed mutexes into structs.

---

## 4.7 Mutable Globals

Avoid mutable package-level state.

Configuration, loggers, stores, clients, registries, managers, etc. should be explicit dependencies.

Prefer dependency injection through constructors.

---

## 4.8 Struct Initialization

Use named struct fields.

Prefer:

```go
cfg := Config{
    Address: ":8080",
}
```

Avoid positional struct literals for non-trivial structs.

---

## 4.9 Naming

Use idiomatic Go names.

Examples:

```text
LogEntry
LogSource
Agent
Server
Client
Store
Parser
Collector
IngestHandler
WebSocketHub
```

Avoid:

```text
LogEntryModel
LogEntryManager
LogEntryProcessorService
LogEntryHandlerImpl
```

Do not use unnecessary `Impl` suffixes.

Keep package names short and meaningful.

Avoid package names such as:

```text
helpers
utils
common
misc
manager
service
```

unless there is an extremely strong reason.

---

# 5. Proposed Repository Structure

Use a maintainable repository layout similar to:

```text
barnacles/
├── cmd/
│   ├── barnacles-server/
│   │   └── main.go
│   └── barnacles-agent/
│       └── main.go
│
├── internal/
│   ├── agent/
│   │   ├── agent.go
│   │   ├── collector.go
│   │   ├── tailer.go
│   │   ├── parser.go
│   │   ├── spool.go
│   │   └── sender.go
│   │
│   ├── ingest/
│   │   ├── handler.go
│   │   └── protocol.go
│   │
│   ├── logentry/
│   │   └── logentry.go
│   │
│   ├── parser/
│   │   ├── parser.go
│   │   ├── text.go
│   │   ├── json.go
│   │   └── regexp.go
│   │
│   ├── server/
│   │   ├── server.go
│   │   ├── routes.go
│   │   └── middleware.go
│   │
│   ├── stream/
│   │   ├── hub.go
│   │   └── client.go
│   │
│   ├── store/
│   │   ├── store.go
│   │   └── filesystem.go
│   │
│   ├── config/
│   │   ├── config.go
│   │   └── loader.go
│   │
│   ├── health/
│   │   └── health.go
│   │
│   └── metrics/
│       └── metrics.go
│
├── web/
│   ├── index.html
│   ├── app.js
│   └── style.css
│
├── config/
│   ├── server.yaml
│   └── agent.yaml
│
├── deployments/
│   ├── docker/
│   └── systemd/
│
├── scripts/
│
├── testdata/
│
├── docs/
│   ├── architecture.md
│   ├── protocol.md
│   ├── configuration.md
│   └── operations.md
│
├── Dockerfile
├── docker-compose.yaml
├── Makefile
├── go.mod
├── go.sum
├── README.md
├── LICENSE
└── .github/
    └── workflows/
        └── ci.yaml
```

Do not create packages merely to match this structure.

If a package does not need to exist, remove it.

Keep dependency direction clear.

Prefer:

```text
cmd -> internal
```

and:

```text
agent -> parser
agent -> sender
server -> ingest
server -> stream
server -> store
```

Avoid circular dependencies.

---

# 6. Core Domain Model

Create a canonical `LogEntry`.

It should contain approximately:

```go
type LogEntry struct {
    ID        string
    Timestamp time.Time
    Host      string
    Source    string
    Level     string
    Message   string
    Fields    map[string]string
}
```

You may refine this design based on implementation needs.

Requirements:

* timestamps must use `time.Time`
* timestamps serialized using RFC3339/RFC3339Nano
* IDs must be unique enough for distributed ingestion
* fields must support structured metadata
* JSON serialization must use explicit field tags
* avoid exposing internal implementation details through exported structs

Example:

```go
type LogEntry struct {
    ID        string            `json:"id"`
    Timestamp time.Time         `json:"timestamp"`
    Host      string            `json:"host"`
    Source    string            `json:"source"`
    Level     string            `json:"level,omitempty"`
    Message   string            `json:"message"`
    Fields    map[string]string `json:"fields,omitempty"`
}
```

---

# 7. Agent Architecture

The agent is responsible for reliable collection.

A source configuration should look similar to:

```yaml
sources:
  - name: nginx-access
    path: /var/log/nginx/access.log
    format: auto

  - name: nginx-error
    path: /var/log/nginx/error.log
    format: text
```

The agent must:

1. Load configuration.
2. Validate configuration.
3. Initialize logging.
4. Initialize metrics.
5. Start collectors.
6. Tail configured files.
7. Detect new data.
8. Detect file rotation.
9. Parse lines.
10. Normalize events.
11. Add host/source metadata.
12. Buffer entries.
13. Batch entries.
14. Send batches to server.
15. Retry failures.
16. Persist unsent entries locally.
17. Recover from temporary server outages.
18. Shutdown gracefully.

---

# 8. File Tailing

Implement robust file tailing.

The tailer must support:

* existing files
* newly created files
* append-only writes
* partial lines
* EOF waiting
* file truncation
* rename-based rotation
* recreation of the original file
* configurable starting position

Configuration should support:

```yaml
start_position: end
```

and:

```yaml
start_position: beginning
```

Do not assume inode behavior is identical across all operating systems.

Implement platform-aware behavior where necessary.

The MVP may initially focus on Linux but code should not unnecessarily hard-code Linux behavior into domain abstractions.

---

# 9. File Rotation

Correctly handle scenarios such as:

```text
app.log
app.log.1
app.log.2
```

and:

```text
app.log -> app.log.1
new app.log created
```

The agent must continue reading the old file until EOF and then begin following the new file.

Do not lose log entries during rotation.

Add tests for:

* normal append
* rotation
* truncation
* recreation
* partial line
* rapid rotation

---

# 10. Parsing

Implement a parser abstraction:

```go
type Parser interface {
    Parse(string) (LogEntry, error)
}
```

Support at minimum:

### Plain text

```text
something happened
```

Result:

```text
Message = "something happened"
```

### JSON logs

Input:

```json
{
  "timestamp": "2026-08-14T12:00:00Z",
  "level": "ERROR",
  "message": "database unavailable"
}
```

Map these into the normalized `LogEntry`.

### Configurable regex parsing

Example:

```yaml
parser:
  type: regexp
  pattern: '^(?P<timestamp>\S+) (?P<level>\S+) (?P<message>.*)$'
```

Keep parser implementations independent.

Malformed log lines should not crash the agent.

Decide and document whether malformed entries:

* become plain-text logs
* are dropped
* are routed to a parser error metric

Prefer preserving the log data rather than silently dropping it.

---

# 11. Delivery Protocol

Do NOT stream agent logs directly to the server using WebSockets.

Use WebSockets primarily for browser/dashboard streaming.

Agent-to-server communication should use HTTP.

Example:

```http
POST /api/v1/ingest
Content-Type: application/json
Authorization: Bearer <token>
```

Request:

```json
{
  "agent_id": "agent-01",
  "events": [
    {
      "id": "event-id",
      "timestamp": "2026-08-14T12:00:00Z",
      "host": "server01",
      "source": "nginx-access",
      "level": "INFO",
      "message": "request completed"
    }
  ]
}
```

The server must respond with explicit success/failure semantics.

Support idempotent processing.

Do not assume that a failed HTTP request means the server received nothing.

The system should tolerate retries without creating uncontrolled duplicates.

---

# 12. Batching

Agents should batch logs before transmission.

Configuration:

```yaml
batch:
  max_events: 500
  flush_interval: 1s
```

Flush when either:

* max event count is reached
* flush interval expires

Use bounded memory.

Never create an unbounded in-memory log queue.

---

# 13. Retry Strategy

Implement exponential backoff.

Example behavior:

```text
1s
2s
4s
8s
16s
30s
30s
...
```

Add jitter.

Do not retry permanently invalid requests indefinitely.

Differentiate:

* temporary network failure
* connection refused
* server unavailable
* HTTP 429
* HTTP 5xx
* HTTP 4xx
* malformed response

Retry only appropriate failures.

---

# 14. Local Durability

The agent must not lose logs simply because the Barnacles server is temporarily unavailable.

Implement a disk-backed spool.

The spool should:

* write batches to disk
* survive process restart
* support retry
* prevent unbounded disk growth
* enforce maximum disk usage
* expose metrics
* recover after restart

Configuration:

```yaml
spool:
  directory: /var/lib/barnacles/spool
  max_size_mb: 1024
```

When disk capacity is exceeded, implement a clearly documented policy.

Possible MVP policy:

```text
oldest data is dropped first
```

but make the behavior explicit and observable.

Do not silently lose data.

---

# 15. Server Storage

The server should persist logs to local filesystem storage for the MVP.

Do not introduce Kafka, Elasticsearch, ClickHouse, Cassandra, or Kubernetes into the core MVP unless there is a compelling reason.

The MVP should remain runnable with:

```bash
./barnacles-server
```

on one machine.

Use append-oriented storage.

Design the storage abstraction so a future implementation can use:

* PostgreSQL
* ClickHouse
* object storage
* Kafka
* another distributed backend

without rewriting the ingestion pipeline.

Example interface:

```go
type LogStore interface {
    Append(context.Context, []LogEntry) error
    Query(context.Context, Query) ([]LogEntry, error)
}
```

---

# 16. Retention

Support basic retention configuration:

```yaml
retention:
  max_size_gb: 10
  max_age_hours: 168
```

Implement cleanup safely.

Cleanup must not block ingestion.

Retention operations should run in a controlled background worker with a cancellable context.

---

# 17. Recent Log Buffer

Maintain a bounded recent-log buffer in memory for WebSocket clients.

For example:

```yaml
stream:
  recent_events: 10000
```

This buffer must have a hard maximum.

New WebSocket clients should be able to receive recent logs before receiving live events.

Do not copy large datasets unnecessarily.

Do not hold mutexes while performing network I/O.

---

# 18. WebSocket Architecture

Implement a WebSocket hub.

Conceptually:

```text
                +----------------+
                | Barnacles Hub  |
                +----------------+
                  /      |      \
                 /       |       \
              client1  client2  client3
```

Requirements:

* concurrent clients
* bounded per-client outbound buffer
* client cancellation
* client cleanup
* ping/pong handling
* write deadlines
* read deadlines where appropriate
* maximum message size
* authentication
* graceful shutdown

A slow browser must not block ingestion.

If a client cannot keep up:

* do not block the entire system
* disconnect the slow client
* record a metric

Do not create unbounded client queues.

---

# 19. WebSocket Protocol

Define an explicit event envelope.

Example:

```json
{
  "type": "log",
  "data": {
    "id": "event-id",
    "timestamp": "2026-08-14T12:00:00Z",
    "host": "server01",
    "source": "nginx",
    "level": "ERROR",
    "message": "connection refused"
  }
}
```

Support:

```text
type = log
type = ping
type = error
```

Keep the protocol documented in:

```text
docs/protocol.md
```

---

# 20. REST API

Implement APIs such as:

```text
GET  /healthz
GET  /readyz
GET  /metrics

POST /api/v1/ingest

GET  /api/v1/logs
GET  /api/v1/sources
GET  /api/v1/agents

GET  /api/v1/stream
```

The actual WebSocket endpoint may be:

```text
GET /ws
```

The API should support basic query filters:

```text
host
source
level
start_time
end_time
search
limit
```

Example:

```text
GET /api/v1/logs?host=server01&level=ERROR&limit=100
```

Limit query complexity in the MVP.

Do not implement a full Elasticsearch-style query language.

---

# 21. Authentication

Implement simple token-based authentication for the MVP.

Example:

```yaml
auth:
  enabled: true
  tokens:
    - ${BARNACLES_TOKEN}
```

Do not hard-code credentials.

Tokens must never be logged.

Support TLS configuration for the server.

Example:

```yaml
tls:
  enabled: true
  cert_file: /etc/barnacles/server.crt
  key_file: /etc/barnacles/server.key
```

For an MVP, token authentication plus TLS is sufficient.

Design the authentication layer so stronger mechanisms can later be added.

---

# 22. Security Requirements

Treat logs as untrusted input.

Protect against:

* oversized requests
* oversized individual log lines
* malformed JSON
* malicious regex patterns
* path traversal
* authentication bypass
* WebSocket resource exhaustion
* memory exhaustion
* disk exhaustion
* header injection
* invalid timestamps

Apply limits:

```text
maximum request body
maximum log line size
maximum batch size
maximum WebSocket message size
maximum number of connections
maximum stored events
maximum spool size
```

All limits must be configurable.

Do not trust client-provided host identity blindly.

---

# 23. HTTP Server

Use idiomatic `net/http`.

Prefer the standard library unless an external router materially improves the project.

Configure:

* read timeout
* read-header timeout
* write timeout
* idle timeout
* maximum request body size

Example:

```go
http.Server{
    Addr:              cfg.Server.Address,
    ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
    ReadTimeout:       cfg.Server.ReadTimeout,
    WriteTimeout:      cfg.Server.WriteTimeout,
    IdleTimeout:       cfg.Server.IdleTimeout,
    Handler:           handler,
}
```

---

# 24. Graceful Shutdown

Both server and agent must shut down cleanly.

Shutdown sequence should resemble:

```text
SIGTERM
   |
   v
stop accepting new work
   |
   v
cancel worker contexts
   |
   v
flush buffers
   |
   v
flush spool/store
   |
   v
close WebSocket clients
   |
   v
wait for goroutines
   |
   v
exit
```

Use `signal.NotifyContext` where appropriate.

Do not call `os.Exit()` from arbitrary packages.

Application termination decisions belong near `main`.

---

# 25. Logging

Implement structured application logging.

Use Go's standard logging facilities where practical.

Prefer structured logs.

Example:

```text
level=INFO component=agent source=nginx event="source started"
```

Application logs must include contextual information.

Never log:

* authentication tokens
* credentials
* sensitive configuration
* entire request bodies unnecessarily

Do not log the same error repeatedly at every layer.

---

# 26. Metrics

Expose Prometheus-compatible metrics.

At minimum:

### Agent

```text
barnacles_agent_events_read_total
barnacles_agent_events_parsed_total
barnacles_agent_parse_errors_total
barnacles_agent_events_sent_total
barnacles_agent_send_errors_total
barnacles_agent_batches_sent_total
barnacles_agent_spool_bytes
barnacles_agent_spool_events
barnacles_agent_sources
```

### Server

```text
barnacles_server_events_ingested_total
barnacles_server_ingest_errors_total
barnacles_server_events_stored_total
barnacles_server_websocket_clients
barnacles_server_websocket_disconnects_total
barnacles_server_events_broadcast_total
barnacles_server_storage_bytes
barnacles_server_query_duration_seconds
barnacles_server_ingest_duration_seconds
```

Use counters, gauges, and histograms appropriately.

Avoid extremely high-cardinality labels.

Do not put:

```text
host
request_id
log_id
message
```

into Prometheus labels unless there is a very strong reason.

---

# 27. Health Endpoints

Implement:

```text
GET /healthz
```

for liveness.

Implement:

```text
GET /readyz
```

for readiness.

Readiness should verify critical dependencies.

Do not make health checks unnecessarily expensive.

---

# 28. Configuration

Use YAML configuration.

Support environment variable overrides for secrets and deployment-specific settings.

Validate configuration on startup.

Fail fast on invalid configuration.

Example:

```yaml
server:
  address: ":8080"

ingest:
  max_batch_events: 500
  max_body_bytes: 10485760

storage:
  directory: "./data/logs"

stream:
  recent_events: 10000
  max_clients: 100

retention:
  max_size_gb: 10
  max_age_hours: 168
```

Configuration must have explicit defaults.

Do not use mutable global configuration.

---

# 29. Dependency Injection

Use explicit constructor-based dependency injection.

Example:

```go
func NewAgent(
    cfg Config,
    parser Parser,
    spool Spool,
    sender Sender,
) *Agent
```

Do not introduce a dependency injection framework.

Go does not need one for this project.

---

# 30. Concurrency Model

Document the concurrency model.

For example:

```text
Agent
 ├── source watcher
 │    └── file tailer
 │
 ├── batcher
 │
 ├── sender
 │
 └── spool worker

Server
 ├── HTTP server
 ├── ingestion handlers
 ├── storage worker
 ├── WebSocket hub
 └── retention worker
```

Every goroutine must have:

* ownership
* lifecycle
* cancellation
* shutdown behavior

Avoid goroutine-per-event designs.

Use bounded concurrency.

---

# 31. Backpressure

Backpressure is a first-class requirement.

Define behavior at every stage:

```text
file -> parser -> batcher -> spool -> network -> server -> storage -> websocket
```

For every queue, document:

* capacity
* producer
* consumer
* blocking behavior
* overflow policy
* shutdown behavior

The system must never allow an unbounded memory queue.

---

# 32. Duplicate Delivery

Assume the following failure:

```text
agent sends batch
server stores batch
network connection dies
agent does not receive response
agent retries batch
```

The implementation must consider duplicate delivery.

Use event IDs and make server storage idempotent where practical.

Document the delivery semantics explicitly.

The MVP should aim for:

```text
at-least-once delivery
```

with duplicate suppression at the server where feasible.

Do NOT claim exactly-once delivery.

---

# 33. Ordering

Document ordering guarantees.

Do not claim global ordering in a distributed system.

The system should preserve:

* per-source ordering where practical
* batch ordering
* timestamp metadata

Explain in documentation that cross-agent ordering is not guaranteed.

---

# 34. Testing

Testing is mandatory.

Target strong coverage of critical behavior rather than artificial 100% coverage.

Implement:

### Unit tests

For:

* config validation
* parsing
* normalization
* batching
* retry logic
* backoff
* spool
* retention
* query filtering
* WebSocket hub behavior
* authentication
* ID generation
* tailing behavior

### Integration tests

Test:

```text
agent -> server -> storage
```

and:

```text
agent -> server -> WebSocket client
```

### Failure tests

Test:

* server unavailable
* server restart
* network failure
* storage failure
* full spool
* malformed logs
* oversized logs
* WebSocket disconnect
* slow WebSocket client
* file rotation
* truncated log file
* SIGTERM during ingestion

### Race tests

All tests must pass:

```bash
go test -race ./...
```

Do not ignore race detector findings.

---

# 35. Test Style

Prefer table-driven tests.

Example:

```go
func TestParseLevel(t *testing.T) {
    tests := []struct {
        name string
        input string
        want string
    }{
        {
            name:  "info",
            input: "INFO",
            want:  "INFO",
        },
        {
            name:  "error",
            input: "ERROR",
            want:  "ERROR",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            ...
        })
    }
}
```

Tests should be deterministic.

Avoid sleeps in tests wherever possible.

Prefer synchronization primitives and controllable clocks where necessary.

---

# 36. Clock Abstraction

Where timing logic requires deterministic tests, consider a small clock abstraction rather than scattering calls to:

```go
time.Now()
```

throughout complex business logic.

Do not create abstractions for trivial code.

Use interfaces only where they materially improve testing or architecture.

---

# 37. Fuzz Testing

Add fuzz tests for security-sensitive parsing.

At minimum fuzz:

```text
JSON log parser
regex parser
line parser
API request decoding
```

The parser must never panic on arbitrary input.

---

# 38. Benchmarking

Create benchmarks for:

* log parsing
* JSON parsing
* batch creation
* serialization
* ingestion
* storage append

Use:

```bash
go test -bench=. ./...
```

Do not optimize without evidence.

---

# 39. Static Analysis and Formatting

The CI pipeline must run at minimum:

```bash
gofmt
go vet ./...
go test ./...
go test -race ./...
```

Also configure a modern Go linter such as `golangci-lint`.

Use static analysis appropriate to current Go versions.

Do not depend on obsolete tooling merely because older versions of the Uber guide mention it.

All code must pass CI.

---

# 40. CI/CD

Create GitHub Actions for:

```text
lint
test
race test
build
security scan
```

CI should run on:

```text
push
pull request
```

Build both:

```text
barnacles-server
barnacles-agent
```

The build must be reproducible.

Use:

```bash
go mod download
go test ./...
go build ./...
```

---

# 41. Containerization

Create a minimal Docker image.

Requirements:

* multi-stage build
* small runtime image
* non-root user
* read-only filesystem where practical
* explicit exposed ports
* health check

Example ports:

```text
8080 HTTP
9090 metrics
```

Do not require a heavyweight base image.

---

# 42. Docker Compose

Provide:

```text
docker-compose.yaml
```

containing:

```text
barnacles-server
barnacles-agent
```

and example log source volumes.

The complete system should be runnable locally with:

```bash
docker compose up
```

A developer should be able to generate logs and immediately see them in the dashboard.

---

# 43. Example Local Demo

Provide a demo application or script:

```text
scripts/generate-logs.sh
```

that generates:

```text
INFO
WARN
ERROR
```

events periodically.

The README should make it possible to demonstrate Barnacles in under five minutes.

---

# 44. Web UI

Build a lightweight web dashboard.

Do not introduce React/Next.js unless absolutely necessary.

Prefer a simple browser application using:

```text
HTML
CSS
JavaScript
```

Features:

### Header

Display:

```text
Barnacles
Connected: YES
Agents: 3
Events/sec: 421
```

### Filters

Support:

```text
Host
Source
Level
Search
```

### Log table

Columns:

```text
Timestamp
Level
Host
Source
Message
```

### Real-time streaming

New logs should appear automatically.

Highlight different log levels appropriately.

### Connection state

Display:

```text
Connected
Connecting
Disconnected
```

Automatically reconnect WebSockets with backoff.

Do not repeatedly reconnect aggressively.

---

# 45. API Versioning

Version public APIs:

```text
/api/v1/...
```

Do not expose internal package structures directly as API contracts.

API schemas should be explicit.

---

# 46. Storage Design

Implement the storage abstraction so that the MVP filesystem implementation can later be replaced.

The filesystem storage should:

* append efficiently
* segment data by time
* avoid keeping entire datasets in memory
* support retention
* support basic querying
* tolerate process restart

Possible structure:

```text
data/
  logs/
    2026/
      08/
        14/
          server01/
            14-00.log
            14-05.log
```

Do not treat the exact directory layout as immutable.

Make storage decisions based on simplicity and predictable operational behavior.

---

# 47. Querying

The MVP query engine only needs:

```text
time range
host
source
level
substring search
limit
```

Avoid building a sophisticated search engine.

Query operations must:

* enforce limits
* avoid unbounded memory allocation
* support cancellation through context
* return predictable errors

---

# 48. Operational Documentation

Create:

```text
docs/architecture.md
docs/protocol.md
docs/configuration.md
docs/operations.md
```

Architecture documentation must explain:

```text
Agent
  |
  | HTTP batch ingestion
  v
Server
  |
  +--> Storage
  |
  +--> Recent Buffer
  |
  +--> WebSocket Hub
           |
           +--> Dashboard
```

Also document:

* delivery semantics
* failure modes
* backpressure
* retention
* security
* scaling limitations
* known limitations
* future architecture

---

# 49. README

Rewrite README.md so a new engineer can understand the project quickly.

Include:

```text
What is Barnacles?
Architecture
Features
Quick Start
Configuration
Running an Agent
Running the Server
API
WebSocket Protocol
Observability
Testing
Docker
Production Deployment
Failure Semantics
Roadmap
Contributing
License
```

Add an architecture diagram.

Explain explicitly what Barnacles is NOT.

For example:

```text
Barnacles is not intended to replace Elasticsearch,
Loki, Splunk, or OpenSearch at large scale in the MVP.
```

The goal is to demonstrate a well-engineered distributed log pipeline.

---

# 50. Makefile

Provide convenient commands:

```text
make build
make test
make race
make lint
make vet
make run-server
make run-agent
make docker
make integration-test
make clean
```

Prefer simple commands that map directly to Go tooling.

---

# 51. Configuration Examples

Provide:

```text
config/server.yaml
config/agent.yaml
```

with safe local defaults.

Do not include real credentials.

Use environment variables for secrets.

---

# 52. Production Defaults

The default configuration should favor safety.

Examples:

* bounded queues
* bounded WebSocket clients
* bounded request sizes
* timeouts enabled
* authentication enabled where practical
* retention enabled
* graceful shutdown
* structured logging
* metrics exposed

Do not choose “unlimited” defaults.

---

# 53. Performance Expectations

The MVP should be designed for a realistic target such as:

```text
10-20 agents
100k events/sec aggregate target
1-5 KB average event size
100 concurrent WebSocket clients
```

These are engineering targets, not promises.

Do not prematurely optimize for millions of events/sec.

Make the architecture capable of being scaled later.

Identify bottlenecks through profiling.

---

# 54. Resource Protection

The system must protect itself from:

* runaway log producers
* malicious clients
* broken agents
* slow browsers
* disk exhaustion
* memory exhaustion
* huge payloads
* high-cardinality metadata

Use explicit limits everywhere.

---

# 55. Avoid Premature Distributed Complexity

Do NOT add:

```text
Kafka
Redis
NATS
Kubernetes
Raft
gRPC
Elasticsearch
ClickHouse
Cassandra
service mesh
```

to the MVP unless a requirement genuinely demands it.

Design extension points for them instead.

The first version should be deployable as:

```text
1 server
+
N agents
+
browser
```

This is deliberate.

---

# 56. Future Architecture

Document a future evolution path:

```text
MVP
 |
 +--> Kafka ingestion
 |
 +--> horizontally scalable collectors
 |
 +--> ClickHouse storage
 |
 +--> object storage
 |
 +--> distributed query layer
 |
 +--> OpenTelemetry integration
 |
 +--> Kubernetes operator
 |
 +--> RBAC
 |
 +--> multi-tenancy
 |
 +--> alerting
 |
 +--> anomaly detection
 |
 +--> log-based SLOs
```

Do NOT implement those features now.

The code should make them plausible future extensions.

---

# 57. OpenTelemetry

Add OpenTelemetry-friendly instrumentation where practical.

At minimum, design the system so request/ingestion tracing can be introduced without changing the domain model.

Expose Prometheus metrics immediately.

Do not force OpenTelemetry into every function.

Avoid observability code overwhelming business logic.

---

# 58. Security Review

Before considering the implementation complete, perform a security-oriented review.

Check:

```text
authentication
authorization boundaries
input validation
resource limits
filesystem paths
file permissions
TLS
credential handling
log injection
WebSocket abuse
request size
regex abuse
disk exhaustion
memory exhaustion
```

Do not claim the system is secure merely because authentication exists.

---

# 59. Failure-Mode Review

Explicitly test and document these scenarios:

### Scenario 1

Agent running.

Server goes down for 10 minutes.

Expected:

```text
logs remain locally buffered
agent continues collecting
agent retries
server comes back
logs drain from spool
```

### Scenario 2

Server stores a batch but response is lost.

Expected:

```text
agent retries
server handles duplicate event IDs safely
```

### Scenario 3

Log rotates while actively writing.

Expected:

```text
no intentional event loss
new file continues being tailed
```

### Scenario 4

WebSocket client stops consuming.

Expected:

```text
server remains healthy
client is eventually disconnected
```

### Scenario 5

Server receives malformed event.

Expected:

```text
request rejected safely
server remains healthy
metrics increment
```

### Scenario 6

Agent receives SIGTERM.

Expected:

```text
stop collecting
flush current batch
flush spool
close connections
exit cleanly
```

---

# 60. Code Review Standard

Before finishing implementation, review every package as a staff engineer would.

Look specifically for:

* unnecessary abstractions
* unnecessary interfaces
* global state
* hidden goroutines
* goroutine leaks
* data races
* unbounded queues
* unchecked errors
* duplicate error logging
* excessive nesting
* confusing names
* overly large functions
* overly large packages
* exported APIs without documentation
* unnecessary pointer usage
* unnecessary allocations
* unnecessary conversions
* unnecessary dependencies
* configuration complexity
* security problems
* shutdown bugs

Prefer deleting code over adding abstractions.

---

# 61. Documentation Standard

Every exported type/function/method must have an appropriate Go doc comment.

Comments should explain:

* why
* constraints
* behavior
* invariants

Do not write comments that merely restate the code.

Bad:

```go
// Start starts the server.
func (s *Server) Start() {}
```

Better:

```go
// Start begins accepting requests and returns when the server exits.
// The caller must cancel the server context to initiate graceful shutdown.
func (s *Server) Start(ctx context.Context) error {}
```

---

# 62. Implementation Order

Implement in this order.

## Phase 1 — Foundation

Create:

```text
go.mod
project structure
configuration
logging
domain models
HTTP server
health endpoints
metrics
```

Run:

```bash
go test ./...
go vet ./...
```

before continuing.

## Phase 2 — Log Collector

Implement:

```text
tailer
parser
normalization
source configuration
```

Add comprehensive tests.

## Phase 3 — Agent Delivery

Implement:

```text
batching
HTTP ingestion client
retry
backoff
spool
```

Test server outage recovery.

## Phase 4 — Server Ingestion

Implement:

```text
ingest API
validation
idempotency
storage
recent buffer
```

## Phase 5 — WebSocket Streaming

Implement:

```text
hub
clients
backpressure
reconnect
authentication
```

## Phase 6 — Dashboard

Implement:

```text
HTML
CSS
JavaScript
filters
streaming
connection status
```

## Phase 7 — Production Hardening

Implement:

```text
TLS
limits
timeouts
retention
security validation
graceful shutdown
```

## Phase 8 — Tooling

Implement:

```text
Docker
Compose
Makefile
GitHub Actions
linting
benchmarks
fuzz tests
documentation
```

---

# 63. Definition of Done

Barnacles is complete only when all of the following are true.

### Build

```bash
go build ./...
```

passes.

### Tests

```bash
go test ./...
```

passes.

### Race detector

```bash
go test -race ./...
```

passes.

### Static analysis

```bash
go vet ./...
```

passes.

### Linting

The configured linter passes.

### Docker

```bash
docker compose up
```

starts the system successfully.

### Functional flow

A log written on an agent machine appears in:

```text
Agent
 -> HTTP ingestion
 -> Server
 -> Storage
 -> WebSocket
 -> Browser
```

### Failure recovery

Stopping the server does not immediately lose agent-collected logs.

### Rotation

Rotated log files continue to be collected.

### Shutdown

SIGTERM produces a clean shutdown.

### Documentation

A new engineer can run the system without reading the source code.

---

# 64. Important Implementation Principle

Do not blindly implement every requirement exactly as written if doing so produces poor Go code.

Use engineering judgment.

For every major design decision ask:

1. Is this required?
2. Is there a simpler solution?
3. Does this create hidden concurrency?
4. Does this introduce unnecessary coupling?
5. Does this create operational risk?
6. Can it be tested?
7. Does it preserve future extensibility?
8. Does it follow idiomatic Go?

Choose the simplest design that satisfies the requirement.

---

# 65. Final Quality Gate

Before declaring the work complete:

1. Build the entire repository.
2. Run all tests.
3. Run race detection.
4. Run static analysis.
5. Run linting.
6. Run benchmarks.
7. Run fuzz tests.
8. Run Docker Compose.
9. Manually test live log streaming.
10. Test server outage and recovery.
11. Test file rotation.
12. Test graceful shutdown.
13. Inspect goroutine lifetimes.
14. Inspect memory growth.
15. Review security boundaries.
16. Review configuration defaults.
17. Review README.
18. Review architecture documentation.

Then perform one final staff-engineer review of the repository.

Do not declare success because the code compiles.

Declare success only when the system behaves correctly under failure and the implementation remains understandable.

---

# 66. Final Deliverable

Produce a complete, runnable repository.

At the end provide:

```text
1. Architecture summary
2. Repository structure
3. Important design decisions
4. Concurrency model
5. Failure semantics
6. Security model
7. Testing strategy
8. Performance considerations
9. Known limitations
10. Future scaling strategy
11. Commands to run locally
12. Example demo workflow
```

Do not merely describe code that should exist.

Actually implement it.

Do not leave TODO placeholders for core functionality.

Do not fake functionality.

Do not silently skip requirements.

If a requirement must be changed because of a technical constraint, document the reason explicitly and choose the simplest production-safe alternative.