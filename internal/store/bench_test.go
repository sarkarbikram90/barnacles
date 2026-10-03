package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

func BenchmarkStoreAppend(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "barnacles-append-bench-*")
	if err != nil {
		b.Fatalf("MkdirTemp: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	fs, err := NewFileStore(Config{Directory: tempDir})
	if err != nil {
		b.Fatalf("NewFileStore: %v", err)
	}
	defer fs.Close()

	ctx := context.Background()
	batch := make([]logentry.LogEntry, 100)
	now := time.Now().UTC()
	for i := range batch {
		batch[i] = logentry.New("node-1", "bench-app", "INFO",
			fmt.Sprintf("user login event payload trace_id=abcdef1234567890 duration_ms=%d", i),
			map[string]string{"env": "prod", "cluster": "us-east-1"})
		batch[i].Timestamp = now
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := fs.Append(ctx, batch); err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}
}

func BenchmarkStoreQueryPruned(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "barnacles-query-bench-*")
	if err != nil {
		b.Fatalf("MkdirTemp: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	fs, err := NewFileStore(Config{Directory: tempDir})
	if err != nil {
		b.Fatalf("NewFileStore: %v", err)
	}
	defer fs.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// Seed 20 batches of INFO logs and 1 batch of ERROR logs
	for i := 0; i < 20; i++ {
		batch := []logentry.LogEntry{
			logentry.New("node-1", "bench-app", "INFO", fmt.Sprintf("info message %d", i), nil),
		}
		batch[0].Timestamp = now.Add(time.Duration(i) * time.Minute)
		_ = fs.Append(ctx, batch)
	}
	errBatch := []logentry.LogEntry{
		logentry.New("node-1", "bench-app", "ERROR", "fatal database connection pool exhausted", nil),
	}
	errBatch[0].Timestamp = now.Add(21 * time.Minute)
	_ = fs.Append(ctx, errBatch)

	q := logentry.Query{
		Level: "ERROR",
		Limit: 10,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results, err := fs.Query(ctx, q)
		if err != nil || len(results) != 1 {
			b.Fatalf("unexpected query result: %v (len=%d)", err, len(results))
		}
	}
}
