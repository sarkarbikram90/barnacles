# Barnacles Architecture

Barnacles is a lightweight, distributed log aggregation and real-time streaming platform in Go.

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

## 1. Concurrency Model

### Agent Concurrency
- **File Tailers**: 1 goroutine per watched file. Each tailer detects append events, file rotation (identity checks), truncation, and partial lines.
- **Log Batcher**: 1 goroutine reading from the bounded internal queue (`chan logentry.LogEntry`, default capacity `5,000`). Flushes on count ceiling (`max_events`) or timer tick (`flush_interval`).
- **Sender / Spool Drainer**: 1 goroutine for draining the disk-backed spool with exponential backoff and full jitter when the central server recovers from an outage. Transmits compressed payloads (`Content-Encoding: zstd` or `gzip`) to conserve network bandwidth.

### Server Concurrency
- **HTTP Ingestion Pipeline**: Managed by Go's standard `net/http` server with bounded request bodies (`http.MaxBytesReader`), transparent wire decompression (`zstd` and `gzip`) with 10MB zip-bomb safeguards, and request timeouts. Supports native `/api/v1/ingest` and OpenTelemetry `/v1/logs`.
- **Idempotency Deduplication**: Thread-safe in-memory LRU/TTL cache (`sync.Mutex`) verifying unique UUIDs and SHA-256 event hashes within a 5-minute sliding window.
- **Storage Subsystem**: Concurrency-safe `FileStore` (`sync.RWMutex`) organizing hourly partitions into Zstandard compressed blocks with append-only companion `index.jsonl` manifests (eliminating $O(N)$ write amplification, with transparent backwards compatibility for legacy `index.json`) and an in-memory partition cache.
- **WebSocket Hub**: Manages active browser client connections. Each client has an independent `writePump` and `readPump`. Broadcasts use non-blocking channel writes delivering batch-oriented envelopes (`BroadcastBatch`, `log_batch`); slow clients whose per-client queues overflow are safely disconnected to prevent backpressure from stalling ingestion.
- **Retention Worker**: 1 background goroutine running on a configurable ticker (`check_interval`), evaluating disk budget and age limits without locking ingestion.

---

## 2. Backpressure & Queue Boundaries

Every queue in Barnacles has an explicit capacity:

| Component | Queue / Buffer | Capacity | Overflow Policy |
| :--- | :--- | :--- | :--- |
| **Agent Queue** | `logCh` | 5,000 events | Bounded channel backpressure to file tailers |
| **Agent Spool** | Disk segment files | 1,024 MB | Oldest segment files pruned on disk limit |
| **Server Dedup** | `DedupCache` | 50,000 IDs | Oldest expired entries pruned on limit |
| **Server Hub** | Per-client channel | 256 batched messages | Slow client disconnected; recorded in Prometheus |
| **Recent Buffer**| Ring buffer | 10,000 events | Oldest entries overwritten (circular FIFO) |

---

## 3. Delivery & Failure Semantics

- **Crash-Safe Checkpoint Watermarks**: File tailers maintain atomic checkpoints on disk (`.checkpoint`) tracking `device_id`, `file_identity/inode`, `file_path`, and `byte_offset`. If the agent crashes or the machine loses power, the tailer resumes reading from the exact byte offset upon restart.
- **Rotation Resilience**: During log rotation (`app.log` -> `app.log.1`), the tailer tracks the file handle's inode identity, reads the rotated file to EOF, and follows the newly created replacement file without missing lines or corrupting offsets.
- **Two-Phase Peek/Commit Spool Protocol**: Batches buffered to the disk spool are leased non-destructively via `Peek()`. The batch is marked as `.inflight` on disk. The file is unlinked only after the server acknowledges successful ingestion (`Commit()`). If sending fails, `Revert()` renames the file back so it remains next in line.
- **Crash Recovery**: If an agent process is killed (SIGKILL / power loss) while sending a leased batch, the spool startup scan automatically restores `.inflight` files back into the active spool, preventing log loss.
- **Strict FIFO Preservation**: To prevent leapfrogging during transient network recovery, the agent enforces unified WAL queuing: whenever backlogged batches exist on disk, incoming live batches are spooled directly to preserve absolute chronological delivery order.
- **Edge-to-Server Wire Compression**: Log senders compress HTTP ingestion payloads using Zstandard Level 3 (or gzip), reducing network bandwidth consumption by 70–85%. The server validates compression headers, decompresses streams dynamically, and guards against decompression bombs.
- **Zstandard Block Compression & Append-Only Indexing**: Partition files store 64KB batches compressed with Zstd Level 3. The append-only `index.jsonl` index records block boundaries, byte offsets, event counts, min/max timestamps, and level bitmasks (`1<<0` for INFO, `1<<1` for WARN, `1<<2` for ERROR). Writes append atomically in $O(1)$ time without rewriting historical index records, while in-memory indexing allows queries to prune non-matching blocks without decompression.
- **Idempotency**: If network timeouts or retries deliver an already-ingested batch, the server's deduplication cache identifies duplicate IDs and skips duplicate disk writes.

---

## 4. Distributed Multi-Node Clustering

Barnacles provides enterprise-scale horizontal scalability via modular clustering primitives:

### Virtual-Node Consistent Hash Ring (`internal/cluster/hashring.go`)
- Physical nodes are mapped across a 64-bit integer ring using 128 virtual node positions (vnodes) per node via `xxhash.Sum64String`.
- Consistent hashing provides uniform key distribution (standard deviation < 15%) across cluster nodes.
- When cluster nodes are added or removed, only $K/N$ keys are relocated, preventing mass data churn.
- Multi-replica placement (`GetNodes(key, R)`) selects $R$ distinct physical machines for fault tolerance and high availability.

### Ingestion Request Router (`internal/cluster/router.go`)
- Ingress gateways and server nodes partition incoming log batches by partition key (default `entry.Host + "/" + entry.Source`).
- Entries designated for the local node are committed directly to `localStore.Append()`.
- Entries designated for remote cluster nodes are batched and dispatched in parallel over high-throughput binary Protobuf connections to peer `/api/v1/ingest` endpoints.

### Scatter-Gather Query Coordinator (`internal/cluster/coordinator.go`)
- **Partition Pruning**: Queries targeting a specific `Host` and `Source` route directly to the designated responsible node in $O(1)$ time, eliminating unnecessary network fanout.
- **Parallel Fanout**: Global or multi-host queries scatter sub-queries concurrently to all cluster nodes with configurable execution timeouts.
- **K-Way Sorted Merge**: Merges timestamped log streams into a unified stream ordered by timestamp descending, deduplicates multi-replica records, and enforces the global query `limit`.
- **Hedged Resilience**: If individual nodes fail or experience network partitions, `AllowPartial: true` returns degraded partial results with detailed per-node error diagnostics.

---

## 5. Dynamic File Discovery

### Glob Pattern Tailer Manager (`internal/tailer/manager.go`)
- Supports dynamic multi-file collection using glob expressions, including recursive double-star patterns (e.g. `/var/log/**/*.log`).
- **Static Base Extraction**: Automatically parses patterns into a fixed root traversal directory and compiled regular expression.
- **Live Lifecycle Tracking**: Periodically scans the directory tree to discover newly created log files (starting from beginning or end) and gracefully terminates tailers when files are removed or rotated away.
- **Multiplexed Fan-In**: Fans in lines from all dynamically discovered files into the agent's central pipeline, injecting origin metadata (`file_path`) into normalized event fields.

---

## 6. High-Performance Wire Protocol

### Binary Protocol Buffers (`internal/protocol/protobuf.go`)
- Supports standard `application/x-protobuf` wire serialization alongside HTTP/JSON.
- Implemented with zero-reflection wire encoding (`google.golang.org/protobuf/encoding/protowire`), achieving:
  - **>1.5M log events/sec** serialization and deserialization per core.
  - **>55% lower decoding latency** compared to standard `encoding/json`.
  - **Transparent content negotiation**: Clients specify `Accept: application/x-protobuf` or `Content-Type: application/x-protobuf` with optional `Content-Encoding: zstd`.



