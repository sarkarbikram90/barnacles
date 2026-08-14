<p align="center">
  <img src="assets/barnacles_banner.jpg" alt="Barnacles Logo Banner" width="850">
</p>

# Barnacles — Production-Grade Distributed Log Aggregator

<p align="center">
  <a href="https://github.com/sarkarbikram90/barnacles/actions/workflows/ci.yaml"><img src="https://github.com/sarkarbikram90/barnacles/actions/workflows/ci.yaml/badge.svg" alt="Unit Tests"></a>
  <a href="https://github.com/sarkarbikram90/barnacles"><img src="https://img.shields.io/badge/Coverage-76.1%25-brightgreen" alt="Coverage Status"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.24%2B-00ADD8?logo=go&logoColor=white" alt="Go Version"></a>
  <a href="docker-compose.yaml"><img src="https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white" alt="Docker Ready"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
  <a href="https://github.com/sarkarbikram90/barnacles/pulls"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs Welcome"></a>
</p>

> *"In the wild, barnacles anchor tenaciously to hulls in rough seas, continuously filtering nutrients from turbulent currents without letting go. In distributed systems, **Barnacles** agents attach to edge server nodes to tenaciously tail, parse, and buffer high-velocity log streams—surviving network storms and server outages with durable local disk spools."*

**Barnacles** is a lightweight, production-grade distributed log aggregation and real-time streaming system written in Go (Go 1.26+). It collects log events from distributed edge agents, normalizes structured data, buffers events durably to disk during network partitions, persists time-segmented logs to storage, and streams live log events to connected browser dashboards via WebSockets.

---

## 🏗 Architecture

```
Agent Node (Server A)                          Central Server
+------------------+                          +-------------------------------------------------+
| Log Files        |                          |                                                 |
| (app.log, etc.)  |                          |                                                 |
+--------+---------+                          |                                                 |
         |                                    |                                                 |
         v                                    |                                                 |
+--------+---------+                          |                                                 |
| File Tailer      |                          |                                                 |
| (Rotation/Trunc) |                          |                                                 |
+--------+---------+                          |                                                 |
         |                                    |                                                 |
         v                                    |                                                 |
+--------+---------+                          |                                                 |
| Log Parser       |                          |                                                 |
| (Text/JSON/Regex)|                          |                                                 |
+--------+---------+                          |                                                 |
         |                                    |                                                 |
         v                                    |                                                 |
+--------+---------+                          |                                                 |
| In-Memory        |                          |                                                 |
| Batcher          |                          |                                                 |
+--------+---------+                          |                                                 |
         |                                    |                                                 |
         v                                    |                                                 |
+--------+---------+     HTTP POST Batch      |  +------------------+                           |
| Ingest Sender    +------------------------->|  | Ingest Handler   |                           |
| & Retry Backoff  |  /api/v1/ingest (Bearer) |  | & Dedup LRU Cache|                           |
+---+----------+---+                          |  +--------+---------+                           |
    |          ^                              |           |                                     |
    | On       | Drain                        |     +-----+----------------+                    |
    | Outage   | On Reconnect                 |     |                      |                    |
    v          |                              |     v                      v                    |
+---+----------+---+                          |  +--+---------------+   +--+---------------+    |
| Durable Disk     |                          |  | Filesystem Store |   | WebSocket Hub    |    |
| Spool Segment    |                          |  | (Time-Segmented) |   | & Recent Buffer  |    |
+------------------+                          |  +--------+---------+   +--------+---------+    |
                                              |           |                      |              |
                                              |           v                      v              |
                                              |  +--------+---------+   +--------+---------+    |
                                              |  | Retention Worker |   | Connected        |    |
                                              |  | (Age/Size Prune) |   | Web Dashboards   |    |
                                              |  +------------------+   +------------------+    |
                                              +-------------------------------------------------+
```

---

## 🎯 What Barnacles Is NOT

Barnacles is intentionally designed as a lightweight, operationally simple log aggregation system.
- **It is NOT a replacement for Elasticsearch, OpenSearch, ClickHouse, or Loki** at massive petabyte scale.
- **It does not require external distributed dependencies** (such as Kafka, Zookeeper, Redis, Cassandra, or Kubernetes) to run.
- The MVP can be completely deployed as **1 single central server binary + N agent binaries**.

---

## ✨ Features

- **Robust Edge File Tailing**: Supports append-only writes, rename-based rotation (`app.log` -> `app.log.1`), truncation detection, partial line accumulation, and configurable starting positions (`beginning` or `end`).
- **Flexible Log Parsing**: Built-in parsers for Plain Text, JSON logs, and Named Regexp capture groups, with an auto-detecting parser that preserves unparseable lines.
- **Local Disk-Backed Spooling**: Automatically buffers log batches to local disk when downstream central servers are unavailable. Recovers and drains seamlessly on reconnection.
- **Idempotent Ingestion & Deduplication**: Sliding-window LRU cache on the server prevents duplicate log entries during network retries.
- **Time-Segmented Storage & Retention**: Persists logs partitioned by date/hour, with background retention workers enforcing size and age limits.
- **Real-Time WebSocket Streaming**: Non-blocking broadcast with ring buffer recent replay and slow-client disconnect protection.
- **Modern Web Dashboard**: Real-time stats, log level coloring, interactive filters (host, source, level, text search), and event inspector modal.
- **Production Observability**: Full Prometheus metrics endpoints (`/metrics`) and health checks (`/healthz`, `/readyz`) on both Agent and Server.

---

## 🚀 Quick Start (Under 5 Minutes)

### Option 1: Using Docker Compose

```bash
# Clone the repository
git clone https://github.com/sarkarbikram90/barnacles.git
cd barnacles

# Start Server, Agent, and mock Log Generator
docker compose up --build
```
Open **http://localhost:8080** in your browser to view logs streaming live!

---

### Option 2: Local Binaries

#### 1. Build the binaries
```bash
go build -o bin/barnacles-server ./cmd/barnacles-server
go build -o bin/barnacles-agent ./cmd/barnacles-agent
```

#### 2. Start the Central Server
```bash
./bin/barnacles-server -config ./config/server.yaml
```

#### 3. Start Mock Log Traffic (in a separate terminal)
On Linux/macOS:
```bash
bash scripts/generate-logs.sh
```
On Windows:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\generate-logs.ps1
```

#### 4. Start the Agent (in a separate terminal)
```bash
./bin/barnacles-agent -config ./config/agent.yaml
```

Open **http://localhost:8080** in your browser!

---

## 🔌 API Reference

| Method | Path | Description | Auth Required |
| :--- | :--- | :--- | :--- |
| `GET` | `/healthz` | Liveness health check | No |
| `GET` | `/readyz` | Readiness health check | No |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint | No |
| `POST` | `/api/v1/ingest` | Batch log event ingestion | Optional Bearer Token |
| `GET` | `/api/v1/logs` | Query stored logs by host, source, level, search | Optional Bearer Token |
| `GET` | `/api/v1/sources` | List all distinct log sources | Optional Bearer Token |
| `GET` | `/api/v1/agents` | List all distinct agent/host names | Optional Bearer Token |
| `GET` | `/ws` | Real-time WebSocket streaming connection | Optional Token |

---

## 🧪 Testing & Verification

Barnacles includes comprehensive unit tests, table-driven test suites, concurrency race verification, benchmarks, and fuzz testing:

```bash
# Run all unit and integration tests
go test -v ./...

# Run race detector (Linux/CI)
go test -v -race ./...

# Run static analysis
go vet ./...

# Run performance benchmarks
go test -bench=. -benchmem ./...

# Run parser fuzz smoke tests
go test -fuzz=^FuzzJSONParser$ -fuzztime=5s ./internal/parser
```

---

## ⚙️ Configuration

Sample configurations are provided in `config/`:
- `config/server.yaml`: Server HTTP port, timeouts, storage directory, retention limits, and streaming parameters.
- `config/agent.yaml`: Watched log file paths, parsing formats, batch sizes, retry parameters, and spool paths.

All configurations support environment variable substitution (e.g. `${BARNACLES_AUTH_TOKEN}`). See [docs/configuration.md](docs/configuration.md) for full reference.

---

## 📖 Documentation

- [Architecture & Concurrency Model](docs/architecture.md)
- [Ingestion & WebSocket Protocol](docs/protocol.md)
- [Configuration Reference](docs/configuration.md)
- [Operations & Troubleshooting](docs/operations.md)

---

## 🗺 Roadmap & Future Scalability

- [ ] Pluggable storage backends (ClickHouse, PostgreSQL, Object Storage).
- [ ] OpenTelemetry trace propagation across ingestion stages.
- [ ] Log-based alerting rules with Webhook/Slack notifications.
- [ ] Agent dynamic log discovery via glob patterns (`/var/log/**/*.log`).

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

<!-- Reference Links -->
[ci-img]: https://github.com/sarkarbikram90/barnacles/actions/workflows/ci.yaml/badge.svg
[ci]: https://github.com/sarkarbikram90/barnacles/actions/workflows/ci.yaml
[cov-img]: https://img.shields.io/badge/Coverage-76.1%25-brightgreen
[cov]: https://github.com/sarkarbikram90/barnacles
