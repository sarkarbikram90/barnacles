# Barnacles Architecture

Barnacles is a lightweight, distributed log aggregation and real-time streaming platform in Go.

```
Agent Node                                     Central Server
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

## 1. Concurrency Model

### Agent Concurrency
- **File Tailers**: 1 goroutine per watched file. Each tailer detects append events, file rotation (identity checks), truncation, and partial lines.
- **Log Batcher**: 1 goroutine reading from the bounded internal queue (`chan logentry.LogEntry`, default capacity `5,000`). Flushes on count ceiling (`max_events`) or timer tick (`flush_interval`).
- **Sender / Spool Drainer**: 1 goroutine for draining the disk-backed spool with exponential backoff and full jitter when the central server recovers from an outage.

### Server Concurrency
- **HTTP Ingestion Pipeline**: Managed by Go's standard `net/http` server with bounded request bodies (`http.MaxBytesReader`) and request timeouts.
- **Idempotency Deduplication**: Thread-safe in-memory LRU/TTL cache (`sync.Mutex`) verifying unique UUIDs within a 5-minute sliding window.
- **Storage Subsystem**: Concurrency-safe `FileStore` with append-only segment writes (`sync.RWMutex`) and read streaming.
- **WebSocket Hub**: Manages active browser client connections. Each client has an independent `writePump` and `readPump`. Broadcasts use non-blocking channel writes; slow clients whose per-client queues overflow are safely disconnected to prevent backpressure from stalling ingestion.
- **Retention Worker**: 1 background goroutine running on a configurable ticker (`check_interval`), evaluating disk budget and age limits without locking ingestion.

---

## 2. Backpressure & Queue Boundaries

Every queue in Barnacles has an explicit capacity:

| Component | Queue / Buffer | Capacity | Overflow Policy |
| :--- | :--- | :--- | :--- |
| **Agent Queue** | `logCh` | 5,000 events | Bounded channel backpressure to file tailers |
| **Agent Spool** | Disk segment files | 1,024 MB | Oldest segment files pruned on disk limit |
| **Server Dedup** | `DedupCache` | 50,000 IDs | Oldest expired entries pruned on limit |
| **Server Hub** | Per-client channel | 256 messages | Slow client disconnected; recorded in Prometheus |
| **Recent Buffer**| Ring buffer | 10,000 events | Oldest entries overwritten (circular FIFO) |

---

## 3. Delivery & Failure Semantics

- **At-Least-Once Delivery**: The agent guarantees all read log lines are delivered to the server or persisted to the local disk spool during downstream outages.
- **Idempotency**: If network failures cause an agent to retry a previously acknowledged batch, the server uses the 5-minute deduplication cache to identify duplicate event IDs and prevent double-writing.
- **Outage Survival**: When the Barnacles server is stopped or unreachable, the agent automatically spools batches to disk. When the server returns, the spool worker drains queued logs in FIFO order.
- **Rotation Resilience**: During log rotation (`app.log` -> `app.log.1`), the tailer continues reading the remaining bytes of the rotated file to EOF before seamlessly following the newly created file.
- **Ordering**: Per-source ordering is preserved within each agent. Timestamps are serialized in UTC RFC3339 format.
