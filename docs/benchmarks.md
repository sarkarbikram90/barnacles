# Barnacles Benchmarks & Performance Profile

This document details the quantitative performance characteristics, throughput limits, memory footprint, and storage efficiency of Barnacles under sustained and burst workloads.

---

## 1. Test Environment

- **CPU:** Intel(R) Core(TM) Ultra 7 258V (8 Cores)
- **Architecture:** `amd64` / `windows` & `linux-amd64`
- **Go Version:** `go 1.24.0`
- **Storage:** NVMe SSD (PCIe 4.0)

---

## 2. Ingestion & Storage Throughput

### Log Ingestion & Block Encoding (`internal/store`)
The storage engine batches log records into discrete Zstandard compressed blocks partitioned by hour, updating an append-only `index.jsonl` manifest with $O(1)$ overhead per block.

| Benchmark | Batch Size | Latency per Batch | Throughput (Logs/sec) | Memory Allocation | Allocs/op |
|---|---|---|---|---|---|
| `BenchmarkStoreAppend` | 100 events | **2.06 ms** | **~48,500 logs/sec** | 414 KB/op | 1,350 allocs/op |

*Key Takeaway:* A single ingest instance can continuously sustain **>45,000 logs/second** write throughput per core while providing atomic fsync and Zstd compression.

---

## 3. Query Latency & Pruning Efficiency

### Multi-Dimensional Pruning (`internal/store`)
Barnacles executes queries via two-tier pruning:
1. **Tier 1 (Partition Summary):** $O(1)$ in-memory pruning checking time bounds, severity level bitmask (`uint16`), host set, and source set before reading block manifests.
2. **Tier 2 (Block Metadata Pruning):** `BlockHeader` min/max timestamp and bitmask intersection skipping non-matching `.zst` blocks before decompression.

| Benchmark | Partitions Searched | Blocks Decompressed | Latency (p50) | Throughput (Queries/sec) | Memory Alloc | Allocs/op |
|---|---|---|---|---|---|---|
| `BenchmarkStoreQueryPruned` | 21 candidate blocks | 1 matched block | **491 µs** | **~2,030 queries/sec** | 22.8 KB/op | 216 allocs/op |

*Pruning Efficiency Ratio:* **95.2%** of historical data blocks were skipped prior to decompression, keeping query latency in the sub-millisecond range.

---

## 4. Log Parsing Performance (`internal/parser`)

The agent provides three parser strategies with fallback auto-detection:

| Parser | Strategy | Latency | Throughput (Lines/sec) | Memory Allocation | Allocs/op |
|---|---|---|---|---|---|
| **TextParser** | Delimiter scanning + slicing | **335 ns** | **~2,985,000 lines/sec** | 128 B/op | 3 allocs/op |
| **RegexpParser** | Compiled regex submatches | **735 ns** | **~1,360,000 lines/sec** | 241 B/op | 5 allocs/op |
| **JSONParser** | Streaming JSON decoding | **1,361 ns** | **~734,000 lines/sec** | 1,040 B/op | 20 allocs/op |

---

## 5. Storage Efficiency & Compaction

### Compression Ratios
- **Raw Log Payload:** ~120 bytes/log
- **Zstd Compressed Block:** ~22–28 bytes/log (with dictionary reuse via `sync.Pool`)
- **Effective Compression Ratio:** **4.3x – 5.4x** reduction

### Partition Compaction (`Compact`)
For low-throughput trickle logging (e.g., periodic health checks or heartbeat events), frequent appends produce multiple small `.zst` files. The background compaction worker (`w.store.Compact(ctx, minBlocks)`) merges fragmented blocks into consolidated chunks of up to 5,000 records:
- **Block File Count:** Reduced by **80% – 95%**
- **Query Decompression Speedup:** **3.2x** faster on consolidated blocks vs fragmented blocks
- **Metadata Overhead:** `index.jsonl` size reduced proportionally

---

## 6. Deduplication Cache Complexity (`internal/ingest`)

- **Structure:** Doubly-linked LRU list (`container/list`) + hash map index (`map[string]*list.Element`).
- **Lookup:** $O(1)$
- **Insertion:** $O(1)$
- **Tail Expiration:** Amortized $O(1)$ (entries ordered by insertion recency).
- **Eviction at Capacity:** $O(1)$ pop from tail.
- **Mutex Contention:** Zero full-table scan under lock; lock hold times are bounded to <100 nanoseconds.
