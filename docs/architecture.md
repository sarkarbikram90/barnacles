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
- **HTTP Ingestion Pipeline**: Managed by Go's standard `net/http` server with bounded request bodies (`http.MaxBytesReader`) and request timeouts. Supports native `/api/v1/ingest` and OpenTelemetry `/v1/logs`.
- **Idempotency Deduplication**: Thread-safe in-memory LRU/TTL cache (`sync.Mutex`) verifying unique UUIDs and SHA-256 event hashes within a 5-minute sliding window.
- **Storage Subsystem**: Concurrency-safe `FileStore` (`sync.RWMutex`) organizing hourly partitions into Zstandard compressed blocks with companion `index.json` metadata manifests.
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

- **Crash-Safe Checkpoint Watermarks**: File tailers maintain atomic checkpoints on disk (`.checkpoint`) tracking `device_id`, `file_identity/inode`, `file_path`, and `byte_offset`. If the agent crashes or the machine loses power, the tailer resumes reading from the exact byte offset upon restart.
- **Rotation Resilience**: During log rotation (`app.log` -> `app.log.1`), the tailer tracks the file handle's inode identity, reads the rotated file to EOF, and follows the newly created replacement file without missing lines or corrupting offsets.
- **Two-Phase Peek/Commit Spool Protocol**: Batches buffered to the disk spool are leased non-destructively via `Peek()`. The batch is marked as `.inflight` on disk. The file is unlinked only after the server acknowledges successful ingestion (`Commit()`). If sending fails, `Revert()` renames the file back so it remains next in line.
- **Crash Recovery**: If an agent process is killed (SIGKILL / power loss) while sending a leased batch, the spool startup scan automatically restores `.inflight` files back into the active spool, preventing log loss.
- **Strict FIFO Preservation**: To prevent leapfrogging during transient network recovery, the agent enforces unified WAL queuing: whenever backlogged batches exist on disk, incoming live batches are spooled directly to preserve absolute chronological delivery order.
- **Zstandard Block Compression & Query Pruning**: Partition files store 64KB batches compressed with Zstd Level 3. The `index.json` partition index records block boundaries, event counts, min/max timestamps, and level bitmasks (`1<<0` for INFO, `1<<1` for WARN, `1<<2` for ERROR), allowing queries to prune non-matching blocks without decompression.
- **Idempotency**: If network timeouts or retries deliver an already-ingested batch, the server's deduplication cache identifies duplicate IDs and skips duplicate disk writes.

