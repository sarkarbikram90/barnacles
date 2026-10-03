package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

func TestBlockEncodingAndDecoding(t *testing.T) {
	now := time.Now().UTC()
	entries := []logentry.LogEntry{
		logentry.New("srv-1", "nginx", "INFO", "GET /api/v1/health 200", map[string]string{"latency_ms": "12"}),
		logentry.New("srv-2", "db", "ERROR", "connection refused", map[string]string{"err_code": "5001"}),
	}
	entries[0].Timestamp = now
	entries[1].Timestamp = now.Add(1 * time.Second)

	header, compressed, err := EncodeBlock(entries)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	if header.RecordCount != 2 {
		t.Fatalf("expected 2 records, got %d", header.RecordCount)
	}
	if (header.LevelMask&LevelInfo) == 0 || (header.LevelMask&LevelError) == 0 {
		t.Fatalf("expected LevelMask to contain INFO and ERROR, got %b", header.LevelMask)
	}
	if len(compressed) >= int(header.UncompressedBytes) && len(compressed) > 500 {
		t.Fatalf("expected compression reduction, compressed=%d, uncompressed=%d",
			len(compressed), header.UncompressedBytes)
	}

	// Decode
	decoded, err := DecodeBlock(compressed)
	if err != nil {
		t.Fatalf("DecodeBlock failed: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 decoded entries, got %d", len(decoded))
	}
	if decoded[0].Message != "GET /api/v1/health 200" || decoded[1].Message != "connection refused" {
		t.Fatalf("unexpected decoded content: %+v", decoded)
	}
}

func TestBlockMetadataPruning(t *testing.T) {
	now := time.Now().UTC()
	header := BlockHeader{
		MinTimestampNano: now.UnixNano(),
		MaxTimestampNano: now.Add(10 * time.Minute).UnixNano(),
		LevelMask:        LevelInfo | LevelWarn, // contains only INFO and WARN
		Hosts:            []string{"web-node-01"},
		Sources:          []string{"nginx-access"},
	}

	// 1. Matches: time overlap + INFO level
	qMatch := logentry.Query{
		StartTime: now.Add(-5 * time.Minute),
		EndTime:   now.Add(5 * time.Minute),
		Level:     "INFO",
		Host:      "web-node-01",
		Source:    "nginx-access",
	}
	if !header.MatchesQuery(qMatch) {
		t.Fatalf("expected block to match query")
	}

	// 2. Prune by time: query ends before block starts
	qEarly := logentry.Query{
		StartTime: now.Add(-10 * time.Minute),
		EndTime:   now.Add(-1 * time.Minute),
	}
	if header.MatchesQuery(qEarly) {
		t.Fatalf("expected block to be pruned due to earlier time")
	}

	// 3. Prune by level: query asks for ERROR, but block only has INFO|WARN
	qError := logentry.Query{
		Level: "ERROR",
	}
	if header.MatchesQuery(qError) {
		t.Fatalf("expected block to be pruned due to level mismatch")
	}

	// 4. Prune by host: query asks for db-node-01
	qHost := logentry.Query{
		Host: "db-node-01",
	}
	if header.MatchesQuery(qHost) {
		t.Fatalf("expected block to be pruned due to host mismatch")
	}

	// 5. Prune by source: query asks for redis
	qSource := logentry.Query{
		Source: "redis",
	}
	if header.MatchesQuery(qSource) {
		t.Fatalf("expected block to be pruned due to source mismatch")
	}
}

func TestFileStoreAppendAndQuery(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "logs")

	fs, err := NewFileStore(Config{
		Directory:   storeDir,
		SyncOnWrite: false,
	})
	if err != nil {
		t.Fatalf("NewFileStore() failed: %v", err)
	}
	defer fs.Close()

	now := time.Now().UTC()
	entries := []logentry.LogEntry{
		{
			ID:        "e1",
			Timestamp: now.Add(-2 * time.Minute),
			Host:      "srv-alpha",
			Source:    "nginx",
			Level:     "INFO",
			Message:   "GET /index.html 200",
		},
		{
			ID:        "e2",
			Timestamp: now.Add(-1 * time.Minute),
			Host:      "srv-alpha",
			Source:    "nginx",
			Level:     "ERROR",
			Message:   "POST /api/v1/checkout 500",
		},
		{
			ID:        "e3",
			Timestamp: now,
			Host:      "srv-beta",
			Source:    "postgres",
			Level:     "WARN",
			Message:   "slow query detected",
			Fields:    map[string]string{"query_ms": "450"},
		},
	}

	if err := fs.Append(context.Background(), entries); err != nil {
		t.Fatalf("Append() failed: %v", err)
	}

	if fs.DiskUsage() <= 0 {
		t.Errorf("expected positive DiskUsage, got %d", fs.DiskUsage())
	}

	// Verify Sources and Hosts
	sources, _ := fs.Sources(context.Background())
	if len(sources) != 2 || sources[0] != "nginx" || sources[1] != "postgres" {
		t.Errorf("unexpected sources: %v", sources)
	}

	hosts, _ := fs.Hosts(context.Background())
	if len(hosts) != 2 || hosts[0] != "srv-alpha" || hosts[1] != "srv-beta" {
		t.Errorf("unexpected hosts: %v", hosts)
	}

	// Query: match all
	resAll, err := fs.Query(context.Background(), logentry.Query{Limit: 10})
	if err != nil {
		t.Fatalf("Query(all) failed: %v", err)
	}
	if len(resAll) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(resAll))
	}

	// Query: filter by host srv-beta
	resHost, err := fs.Query(context.Background(), logentry.Query{Host: "srv-beta"})
	if err != nil {
		t.Fatalf("Query(host) failed: %v", err)
	}
	if len(resHost) != 1 || resHost[0].ID != "e3" {
		t.Fatalf("expected e3, got: %v", resHost)
	}

	// Query: filter by level ERROR
	resLevel, err := fs.Query(context.Background(), logentry.Query{Level: "error"})
	if err != nil {
		t.Fatalf("Query(level) failed: %v", err)
	}
	if len(resLevel) != 1 || resLevel[0].ID != "e2" {
		t.Fatalf("expected e2, got: %v", resLevel)
	}

	// Query: substring search
	resSearch, err := fs.Query(context.Background(), logentry.Query{Search: "slow query"})
	if err != nil {
		t.Fatalf("Query(search) failed: %v", err)
	}
	if len(resSearch) != 1 || resSearch[0].ID != "e3" {
		t.Fatalf("expected e3, got: %v", resSearch)
	}
}

func TestFileStorePruning(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "logs")

	fs, err := NewFileStore(Config{
		Directory: storeDir,
	})
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}
	defer fs.Close()

	entries := []logentry.LogEntry{
		logentry.New("host1", "app", "INFO", "msg 1", nil),
		logentry.New("host1", "app", "INFO", "msg 2", nil),
	}
	_ = fs.Append(context.Background(), entries)

	// Prune by size (budget 1 byte)
	deleted, freed, err := fs.Prune(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("Prune() failed: %v", err)
	}
	if deleted != 1 || freed <= 0 {
		t.Fatalf("expected 1 file deleted, got deleted=%d freed=%d", deleted, freed)
	}
}

func TestRetentionWorker(t *testing.T) {
	tempDir := t.TempDir()
	fs, _ := NewFileStore(Config{Directory: filepath.Join(tempDir, "logs")})
	defer fs.Close()

	_ = fs.Append(context.Background(), []logentry.LogEntry{
		logentry.New("host1", "app", "INFO", "msg", nil),
	})

	worker := NewRetentionWorker(fs, config.RetentionSettings{
		Enabled:       true,
		MaxSizeGB:     1,
		MaxAgeHours:   1,
		CheckInterval: 20 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		worker.Start(ctx)
		close(workerDone)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatalf("retention worker did not exit on cancel")
	}
}

func TestAppendOnlyIndexJSONL(t *testing.T) {
	tempDir := t.TempDir()
	partitionDir := filepath.Join(tempDir, "2026", "09", "28", "15")

	h1 := BlockHeader{
		ID:               "blk-1",
		FileName:         "block_1.zst",
		MinTimestampNano: 1000,
		MaxTimestampNano: 2000,
		RecordCount:      10,
		Hosts:            []string{"host-a"},
		Sources:          []string{"src-a"},
	}
	h2 := BlockHeader{
		ID:               "blk-2",
		FileName:         "block_2.zst",
		MinTimestampNano: 2001,
		MaxTimestampNano: 3000,
		RecordCount:      20,
		Hosts:            []string{"host-b"},
		Sources:          []string{"src-b"},
	}

	if err := AppendBlockToPartition(partitionDir, h1, []byte("data1"), false); err != nil {
		t.Fatalf("first append failed: %v", err)
	}
	if err := AppendBlockToPartition(partitionDir, h2, []byte("data2"), false); err != nil {
		t.Fatalf("second append failed: %v", err)
	}

	// Verify index.jsonl exists and has 2 lines
	jsonlPath := filepath.Join(partitionDir, "index.jsonl")
	data, err := os.ReadFile(jsonlPath)
	if err != nil {
		t.Fatalf("read index.jsonl failed: %v", err)
	}
	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	if lines != 2 {
		t.Fatalf("expected 2 lines in index.jsonl, got %d", lines)
	}

	// Load partition index
	headers, err := LoadPartitionIndex(partitionDir)
	if err != nil {
		t.Fatalf("LoadPartitionIndex failed: %v", err)
	}
	if len(headers) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(headers))
	}
	if headers[0].ID != "blk-1" || headers[1].ID != "blk-2" {
		t.Fatalf("unexpected headers order or content: %+v", headers)
	}
}

func TestLegacyIndexJSONBackwardsCompatibility(t *testing.T) {
	tempDir := t.TempDir()
	partitionDir := filepath.Join(tempDir, "2026", "09", "28", "15")
	if err := os.MkdirAll(partitionDir, 0o750); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	legacyHeader := BlockHeader{
		ID:               "legacy-1",
		FileName:         "block_legacy.zst",
		MinTimestampNano: 500,
		MaxTimestampNano: 999,
		RecordCount:      5,
		Hosts:            []string{"legacy-host"},
		Sources:          []string{"legacy-src"},
	}

	// Write legacy index.json
	legacyJSON, err := json.Marshal([]BlockHeader{legacyHeader})
	if err != nil {
		t.Fatalf("marshal legacy header failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(partitionDir, "index.json"), legacyJSON, 0o600); err != nil {
		t.Fatalf("write legacy index.json failed: %v", err)
	}

	// Append a new block using AppendBlockToPartition (writes index.jsonl)
	newHeader := BlockHeader{
		ID:               "new-1",
		FileName:         "block_new.zst",
		MinTimestampNano: 1000,
		MaxTimestampNano: 1500,
		RecordCount:      15,
		Hosts:            []string{"new-host"},
		Sources:          []string{"new-src"},
	}
	if err := AppendBlockToPartition(partitionDir, newHeader, []byte("newdata"), false); err != nil {
		t.Fatalf("AppendBlockToPartition failed: %v", err)
	}

	// LoadPartitionIndex should seamlessly load both legacy and new headers
	headers, err := LoadPartitionIndex(partitionDir)
	if err != nil {
		t.Fatalf("LoadPartitionIndex failed: %v", err)
	}
	if len(headers) != 2 {
		t.Fatalf("expected 2 headers from combined legacy and new index, got %d", len(headers))
	}
	if headers[0].ID != "legacy-1" || headers[1].ID != "new-1" {
		t.Fatalf("unexpected headers order: %+v", headers)
	}
}

func TestPartitionSummaryPruning(t *testing.T) {
	tempDir := t.TempDir()
	fs, err := NewFileStore(Config{Directory: tempDir})
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}
	defer fs.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// Hour 1: INFO logs only from host-a
	hour1 := now.Add(-2 * time.Hour)
	entries1 := []logentry.LogEntry{
		logentry.New("host-a", "service-1", "INFO", "info log 1", nil),
		logentry.New("host-a", "service-1", "INFO", "info log 2", nil),
	}
	entries1[0].Timestamp = hour1
	entries1[1].Timestamp = hour1.Add(5 * time.Minute)
	if err := fs.Append(ctx, entries1); err != nil {
		t.Fatalf("Append hour 1 failed: %v", err)
	}

	// Hour 2: ERROR logs from host-b
	hour2 := now.Add(-1 * time.Hour)
	entries2 := []logentry.LogEntry{
		logentry.New("host-b", "service-2", "ERROR", "error log 1", nil),
	}
	entries2[0].Timestamp = hour2
	if err := fs.Append(ctx, entries2); err != nil {
		t.Fatalf("Append hour 2 failed: %v", err)
	}

	// Verify partition summaries were constructed
	fs.mu.RLock()
	if len(fs.partitionSummaries) != 2 {
		t.Fatalf("expected 2 partition summaries, got %d", len(fs.partitionSummaries))
	}
	fs.mu.RUnlock()

	// Query for ERROR only — should return only entries from hour 2
	errResults, err := fs.Query(ctx, logentry.Query{Level: "ERROR", Limit: 10})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(errResults) != 1 || errResults[0].Message != "error log 1" {
		t.Fatalf("unexpected error results: %+v", errResults)
	}

	// Query for host-a only — should return only entries from hour 1
	hostResults, err := fs.Query(ctx, logentry.Query{Host: "host-a", Limit: 10})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(hostResults) != 2 {
		t.Fatalf("expected 2 results for host-a, got %d", len(hostResults))
	}

	// Query for nonexistent host — should return 0 without scanning blocks
	emptyResults, err := fs.Query(ctx, logentry.Query{Host: "nonexistent", Limit: 10})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(emptyResults) != 0 {
		t.Fatalf("expected 0 results for nonexistent host, got %d", len(emptyResults))
	}
}

func TestBlockCompaction(t *testing.T) {
	tempDir := t.TempDir()
	fs, err := NewFileStore(Config{Directory: tempDir})
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}
	defer fs.Close()

	ctx := context.Background()
	baseTime := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// Append 5 separate small trickle batches in the same hour partition
	for i := 0; i < 5; i++ {
		entry := logentry.New("srv-1", "app", "INFO", "trickle log", map[string]string{"batch": string(rune('A' + i))})
		entry.Timestamp = baseTime.Add(time.Duration(i) * time.Minute)
		if err := fs.Append(ctx, []logentry.LogEntry{entry}); err != nil {
			t.Fatalf("Append batch %d failed: %v", i, err)
		}
	}

	partitionDir := filepath.Join(tempDir, "2026", "10", "01", "12")

	// Verify 5 blocks exist before compaction
	fs.mu.RLock()
	preHeaders := fs.partitionCache[partitionDir]
	fs.mu.RUnlock()
	if len(preHeaders) != 5 {
		t.Fatalf("expected 5 blocks before compaction, got %d", len(preHeaders))
	}

	// Run compaction: threshold minBlocks=2
	reduced, freed, err := fs.Compact(ctx, 2)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	if reduced != 4 {
		t.Fatalf("expected 4 blocks reduced (5 -> 1), got %d (freed=%d)", reduced, freed)
	}

	// Verify only 1 consolidated block remains
	fs.mu.RLock()
	postHeaders := fs.partitionCache[partitionDir]
	fs.mu.RUnlock()
	if len(postHeaders) != 1 {
		t.Fatalf("expected 1 block after compaction, got %d", len(postHeaders))
	}

	// Verify all 5 entries can be queried with complete data fidelity
	results, err := fs.Query(ctx, logentry.Query{Limit: 10})
	if err != nil {
		t.Fatalf("Query after compaction failed: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 entries after compaction, got %d", len(results))
	}
}

func TestTieredArchivingAndRestoration(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "store")
	archiveDir := filepath.Join(tempDir, "archive")

	fs, err := NewFileStore(Config{Directory: storeDir})
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}
	defer fs.Close()

	archive, err := NewLocalArchiveStore(archiveDir)
	if err != nil {
		t.Fatalf("NewLocalArchiveStore failed: %v", err)
	}

	ctx := context.Background()
	baseTime := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	entries := []logentry.LogEntry{
		logentry.New("node-1", "syslog", "INFO", "archive test log 1", nil),
		logentry.New("node-1", "syslog", "WARN", "archive test log 2", nil),
	}
	entries[0].Timestamp = baseTime
	entries[1].Timestamp = baseTime.Add(10 * time.Minute)

	if err := fs.Append(ctx, entries); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	partitionDir := filepath.Join(storeDir, "2026", "10", "01", "10")
	archiver := NewTieredArchiver(fs, archive, 1*time.Hour)

	// Archive the partition
	uploaded, bytesArchived, err := archiver.ArchivePartition(ctx, partitionDir)
	if err != nil {
		t.Fatalf("ArchivePartition failed: %v", err)
	}
	if uploaded < 2 || bytesArchived <= 0 {
		t.Fatalf("expected at least 2 files uploaded (block + index), got %d (bytes=%d)",
			uploaded, bytesArchived)
	}

	// Create a new clean FileStore to test restoration from object store
	restoreStoreDir := filepath.Join(tempDir, "restored_store")
	restoredFs, err := NewFileStore(Config{Directory: restoreStoreDir})
	if err != nil {
		t.Fatalf("NewFileStore for restore failed: %v", err)
	}
	defer restoredFs.Close()

	restoreArchiver := NewTieredArchiver(restoredFs, archive, 1*time.Hour)
	restoredCount, restoredBytes, err := restoreArchiver.RestorePartition(ctx, "2026/10/01/10")
	if err != nil {
		t.Fatalf("RestorePartition failed: %v", err)
	}
	if restoredCount != uploaded || restoredBytes != bytesArchived {
		t.Fatalf("restoration mismatch: uploaded %d (bytes %d), restored %d (bytes %d)",
			uploaded, bytesArchived, restoredCount, restoredBytes)
	}

	// Query restored FileStore
	restoredEntries, err := restoredFs.Query(ctx, logentry.Query{Limit: 10})
	if err != nil {
		t.Fatalf("Query on restored store failed: %v", err)
	}
	if len(restoredEntries) != 2 {
		t.Fatalf("expected 2 restored entries, got %d", len(restoredEntries))
	}
}
