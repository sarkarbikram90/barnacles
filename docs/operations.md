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
- **Behavior**: The agent's HTTP sender classifies the error as retryable (`ErrTemporary`), switches to exponential backoff with jitter, and writes log batches into the on-disk spool (`./data/agent-spool`).
- **Recovery**: Once the server returns, the agent's background spool worker automatically drains buffered segment files in FIFO order without data loss.

### Scenario 2: Log File Rotation
- **Observation**: An application rotates `app.log` to `app.log.1` and opens a fresh `app.log`.
- **Behavior**: The tailer finishes draining any unread bytes in the old rotated file to EOF, closes the old handle, opens the new file at byte offset 0, and continues streaming new lines seamlessly.

### Scenario 3: Slow WebSocket Browser Client
- **Observation**: A browser tab is throttled or stalls on the network.
- **Behavior**: The server's broadcast logic detects that the client's 256-message buffer is full. The slow client is disconnected, freeing server memory and preventing backpressure from blocking central ingestion. The event is recorded in `barnacles_server_websocket_disconnects_total{reason="buffer_overflow"}`.

### Scenario 4: Disk Storage Retention
- **Observation**: Disk usage approaches `max_size_gb` or logs exceed `max_age_hours`.
- **Behavior**: The retention worker inspects time-segmented log files and deletes the oldest hourly/daily segments until storage is within budget.
