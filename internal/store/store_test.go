package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

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
