package store

import (
	"context"
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
	if (header.LevelMask & LevelInfo) == 0 || (header.LevelMask & LevelError) == 0 {
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
		LevelMask:         LevelInfo | LevelWarn, // contains only INFO and WARN
		Hosts:             []string{"web-node-01"},
		Sources:           []string{"nginx-access"},
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
