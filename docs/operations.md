# Barnacles Operations & Troubleshooting Guide

This guide covers operational practices, observability, Prometheus metrics, troubleshooting scenarios, and failure recovery.

---

## 1. Observability & Metrics

Both Barnacles Server and Agent expose Prometheus-compatible metrics.

### Key Server Metrics
| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `barnacles_server_events_ingested_total` | Counter | Total log events received via HTTP ingest |
| `barnacles_server_ingest_errors_total` | Counter | Total validation and storage errors |
| `barnacles_server_events_stored_total` | Counter | Total events persisted to disk |
| `barnacles_server_websocket_clients` | Gauge | Currently connected WebSocket clients |
| `barnacles_server_websocket_disconnects_total` | Counter | Disconnects by reason (e.g. `buffer_overflow`) |
| `barnacles_server_events_broadcast_total` | Counter | Total events broadcast to WebSocket streams |
| `barnacles_server_storage_bytes` | Gauge | Total bytes occupied by stored logs on disk |
| `barnacles_server_ingest_duration_seconds` | Histogram | Request latency for `/api/v1/ingest` |
| `barnacles_server_query_duration_seconds` | Histogram | Latency for log query executions |

### Key Agent Metrics
| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `barnacles_agent_events_read_total` | Counter | Raw lines read from watched files (by source) |
| `barnacles_agent_events_parsed_total` | Counter | Successfully parsed events (by source) |
| `barnacles_agent_parse_errors_total` | Counter | Parsing errors (by source) |
| `barnacles_agent_events_sent_total` | Counter | Successfully transmitted events |
| `barnacles_agent_send_errors_total` | Counter | HTTP delivery failures |
| `barnacles_agent_batches_sent_total` | Counter | Total batches delivered |
| `barnacles_agent_spool_bytes` | Gauge | Current size of on-disk spool in bytes |
| `barnacles_agent_spool_events` | Gauge | Number of events buffered in disk spool |
| `barnacles_agent_sources` | Gauge | Number of actively watched file sources |

---

## 2. Health & Readiness Probes

- **Liveness Probe**: `GET /healthz` returns `200 OK` and uptime if the binary is running.
- **Readiness Probe**: `GET /readyz` verifies storage availability and critical dependencies.

---

## 3. Operational Scenarios & Failure Recovery

### Scenario 1: Central Server Outage
- **Observation**: Server becomes unreachable or returns HTTP 503.
- **Behavior**: The agent's HTTP sender classifies the error as retryable (`ErrTemporary`), switches to exponential backoff with jitter, and writes log batches into the on-disk spool (`./data/agent-spool`). Live incoming logs are routed directly into the spool to guarantee strict chronological FIFO order without leapfrogging.
- **Recovery**: Once the server returns, the agent's background spool worker automatically leases batches via `Peek()`, delivers them, and calls `Commit()` only upon HTTP 200 acknowledgment.

### Scenario 2: Log File Rotation
- **Observation**: An application rotates `app.log` to `app.log.1` and opens a fresh `app.log`.
- **Behavior**: The tailer tracks the file handle's OS-level identity (`device_id` + `inode`). It finishes draining any unread bytes in the old rotated file to EOF, detects the new inode at the target path, resets its offset to 0, and continues streaming new lines seamlessly.

### Scenario 3: Agent Crash Mid-Flight (SIGKILL / Sudden Power Cut)
- **Observation**: The agent process is abruptly killed or the server loses power while a batch is in transit to the central server.
- **Behavior**: Because Barnacles uses a two-phase `Peek` $\to$ `Send` $\to$ `Commit` lease protocol, batches are never deleted prior to server confirmation. In-flight batches are held on disk with `.inflight` extensions.
- **Recovery**: Upon agent restart, the spool manager scans the spool directory, automatically identifies orphaned `.inflight` files, and restores them to the active FIFO queue at index 0. No data is lost.

### Scenario 4: Machine Reboot & Checkpoint Watermark Resumption
- **Observation**: An edge node reboots, restarting the Barnacles agent process.
- **Behavior**: Each watched file has a companion `.checkpoint` file atomically written (`.checkpoint.tmp` $\to$ `.checkpoint`) tracking `device_id`, `inode`, `file_path`, and exact `byte_offset`.
- **Recovery**: The tailer inspects the checkpoint, verifies the inode identity of the file, and seeks directly to the saved byte offset. Logs appended while the agent was down are ingested immediately without re-reading previously processed lines.

### Scenario 5: Slow WebSocket Browser Client
- **Observation**: A browser tab is throttled or stalls on the network.
- **Behavior**: The server broadcasts log events using high-efficiency batch envelopes (`log_batch`), minimizing framing overhead. If a client stalls and its 256-message buffer overflows, the slow client is disconnected, freeing server memory and preventing backpressure from blocking central ingestion. The event is recorded in `barnacles_server_websocket_disconnects_total{reason="buffer_overflow"}`.

### Scenario 6: Compressed Storage & Background Retention
- **Observation**: Disk usage approaches `max_size_gb` or logs exceed `max_age_hours`.
- **Behavior**: Storage partition segments are compressed into 64KB Zstandard blocks accompanied by append-only `index.jsonl` manifests (with transparent backwards compatibility for legacy `index.json`). Block index metadata is appended in $O(1)$ time, completely avoiding $O(N)$ disk write amplification. The background retention worker prunes the oldest hourly partitions, deleting both the data blocks and index entries while keeping memory and storage bounded.

### Scenario 7: OpenTelemetry Collector or SDK Ingestion
- **Observation**: Applications or external OpenTelemetry collectors stream telemetry to Barnacles.
- **Behavior**: Direct HTTP POST requests to `/v1/logs` are received by the native OTLP handler, parsed from `ExportLogsServiceRequest` JSON, deduplicated against the central 5-minute sliding LRU cache, persisted to the Zstd block store, and broadcasted in real time to connected WebSocket dashboards.

### Scenario 8: Edge-to-Server Wire Compression & Network Bandwidth Savings
- **Observation**: High log volume from multiple agents causes significant network egress or server ingest bandwidth saturation.
- **Behavior**: Agents stream log batches using Zstandard Level 3 compression (`Content-Encoding: zstd`, or optional `gzip`), reducing payload size by 70–85%. The central server inspects `Content-Encoding`, decompresses payloads on the fly with a 10MB decompression ceiling, and gracefully handles uncompressed requests.


