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

**Barnacles** is a lightweight, production-grade distributed log aggregation and real-time streaming system written in Go. It collects log events from distributed edge agents, normalizes structured data, buffers events durably to disk during network partitions, persists time-segmented logs to storage, and streams live log events to connected browser dashboards via WebSockets.

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
| + Checkpoints    | <--- (.checkpoint)       |                                                 |
| (inode + offset) |      [device/inode/pos]  |                                                 |
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
| Batcher & WAL    |                          |                                                 |
+--------+---------+                          |                                                 |
         |                                    |                                                 |
         v                                    |                                                 |
+--------+---------+   HTTP POST /api/v1/ingest|  +------------------+                           |
| Ingest Sender    +------------------------->|  | Ingest Handler   |                           |
| & (Zstd/Gzip Wire|   (zstd/gzip compressed) |  | & Decompression  |                           |
|  Compression)    |   or OTLP POST /v1/logs  |  | & Dedup LRU Cache|                           |
+---+----------+---+                          |  +--------+---------+                           |
    |          ^                              |           |                                     |
    | On       | Drain                        |     +-----+----------------+                    |
    | Outage   | (Peek/Ack/Commit)            |     |                      |                    |
    v          |                              |     v                      v                    |
+---+----------+---+                          |  +--+---------------+   +--+---------------+    |
| Durable Spool    |                          |  | Zstd Block Store |   | WebSocket Hub    |    |
| (.inflight lease)|                          |  | + index.jsonl    |   | (Batch Broadcast)|    |
| (Strict FIFO)    |                          |  | (Pruned Queries) |   | (Non-blocking)   |    |
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

- **Distributed Multi-Node Clustering**: Enterprise horizontal scaling with virtual-node consistent hashing (`xxhash`), dynamic node addition/removal with minimal key movement ($K/N$), cluster ingestion routing, and scatter-gather query aggregation with partition pruning and k-way sorted merge.
- **High-Performance Protocol Buffers Wire Format**: Zero-reflection proto3 binary wire protocol (`application/x-protobuf`) achieving **>1.5M events/sec** per core with transparent content negotiation and >55% lower decoding latency.
- **Dynamic Glob File Discovery**: Built-in `TailerManager` supporting recursive double-star wildcards (e.g. `/var/log/**/*.log`), automatically discovering new log files at runtime and tearing down deleted ones.
- **Crash-Safe Edge File Tailing**: Persistent watermark checkpoints (`device_id`, `file_identity/inode`, `byte_offset`) committed atomically to disk; automatically survives agent crashes, hard reboots, and log file rotations (`app.log` -> `app.log.1`).
- **Peek -> Send -> Commit Disk Spooling**: Batches are never unlinked before central server ACK. In-flight leases are recovered on crash restart, fsync writes ensure crash durability, and strict FIFO order is preserved across network partitions.
- **Edge-to-Server Wire Compression**: Payloads are compressed with Zstandard Level 3 (or gzip) prior to HTTP transmission, reducing network egress bandwidth by 70–85% with server-side decompression bomb guards.
- **Append-Only Block Storage & Compaction**: Server storage organizes hourly partitions into Zstandard-compressed blocks with $O(1)$ append-only `index.jsonl` manifests and multi-dimensional partition summaries ($O(1)$ time, severity bitmask, host, and source pruning). Background partition compaction merges trickle writes to eliminate small-file fragmentation.
- **Tiered Cold Storage**: Pluggable `ObjectStore` backend interface and `TieredArchiver` for offloading sealed historical partitions to cloud object stores (S3 / GCS / MinIO) or local archives.
- **OpenTelemetry Native Ingestion**: Supports standard OTLP/HTTP JSON (`POST /v1/logs`) as well as native Barnacles batches (`POST /api/v1/ingest`) with $O(1)$ LRU sliding-window deduplication.
- **Flexible Log Parsing**: Built-in parsers for Plain Text, JSON logs, and Named Regexp capture groups, with an auto-detecting parser that preserves unparseable lines.
- **Event-Driven & Polling Hybrid Tailing**: File tailer supports sub-millisecond event-driven wakeups (`Wakeup()`) with periodic fallback polling.
- **Fault-Injection Chaos Verified**: 10-scenario chaos harness verifying zero acknowledged loss, per-source FIFO preservation, and crash recovery between `Peek()` and `Commit()`.
- **High-Throughput WebSocket Streaming**: Batch-oriented WebSocket delivery (`log_batch`) eliminating per-message framing overhead, non-blocking broadcasts with ring buffer recent replay, and slow-client disconnect protection.
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
| `POST` | `/api/v1/ingest` | Native batch log event ingestion | Optional Bearer Token |
| `POST` | `/v1/logs` | OpenTelemetry OTLP/HTTP JSON log ingestion | Optional Bearer Token |
| `GET` | `/api/v1/logs` | Query stored logs by host, source, level, search | Optional Bearer Token |
| `GET` | `/api/v1/sources` | List all distinct log sources | Optional Bearer Token |
| `GET` | `/api/v1/agents` | List all distinct agent/host names | Optional Bearer Token |
| `GET` | `/ws` | Real-time WebSocket streaming connection | Optional Token |

---

## 🧪 Testing & Verification

Barnacles includes comprehensive unit tests, table-driven test suites, the **Barnacles Reliability Test Suite** (validating crash recovery before ACK, rotation resumption, and strict FIFO ordering under network partitions), concurrency race verification, benchmarks, and fuzz testing:

```bash
# Run all unit and integration tests (including Reliability Suite)
go test -v ./...

# Run the dedicated Barnacles Reliability Test Suite
go test -v -run TestStrictFIFO ./internal/agent/...
go test -v -run TestCrash ./internal/agent/...

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
- [Benchmarks & Performance Profile](docs/benchmarks.md)
- [Ingestion & WebSocket Protocol](docs/protocol.md)
- [Configuration Reference](docs/configuration.md)
- [Operations & Troubleshooting](docs/operations.md)

---

## 🗺 Roadmap & Future Scalability

- [x] OpenTelemetry native ingestion (`POST /v1/logs` HTTP/JSON).
- [x] Persistent watermark tailer checkpoints (`device_id`, `inode`, `offset`).
- [x] Non-destructive two-phase spool lease (`Peek` $\to$ `Send` $\to$ `Commit`).
- [x] Zstandard block storage with $O(1)$ append-only `index.jsonl` manifests.
- [x] Edge-to-server transparent wire compression (`Content-Encoding: zstd` / `gzip`).
- [x] High-throughput batch-oriented WebSocket streaming (`log_batch`).
- [x] Multi-dimensional $O(1)$ partition summary pruning and block level filtering.
- [x] Background partition block compaction (resolving the small-file problem).
- [x] Long-term object storage tiering abstraction (`ObjectStore` / `TieredArchiver`).
- [x] Event-driven tailer notification (`Wakeup`) with fallback polling.
- [x] 10-scenario fault-injection chaos verification suite.
- [ ] Agent dynamic log discovery via glob patterns (`/var/log/**/*.log`).
- [ ] Central fleet management & remote Over-The-Air (OTA) configuration.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

<!-- Reference Links -->
[ci-img]: https://github.com/sarkarbikram90/barnacles/actions/workflows/ci.yaml/badge.svg
[ci]: https://github.com/sarkarbikram90/barnacles/actions/workflows/ci.yaml
[cov-img]: https://img.shields.io/badge/Coverage-76.1%25-brightgreen
[cov]: https://github.com/sarkarbikram90/barnacles
