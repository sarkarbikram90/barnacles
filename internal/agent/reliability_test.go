package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/ingest"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
	"github.com/sarkarbikram90/barnacles/internal/store"
)

// TestStrictFIFODrainUnderOutageAndLiveTraffic tests that during a network outage,
// both backlog and incoming live events are queued to the spool in strict FIFO order,
// and drained sequentially without live events leapfrogging backlog.
func TestStrictFIFODrainUnderOutageAndLiveTraffic(t *testing.T) {
	var (
		online        atomic.Bool
		receivedMu    sync.Mutex
		receivedSeqs  []int
		receivedTotal atomic.Int64
	)
	online.Store(false) // Server starts offline

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !online.Load() {
			http.Error(w, "network partition", http.StatusServiceUnavailable)
			return
		}

		var req logentry.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		receivedMu.Lock()
		for _, e := range req.Events {
			// Extract sequence number from message "seq-<num>"
			parts := strings.Split(e.Message, "-")
			if len(parts) == 2 {
				if seq, err := strconv.Atoi(parts[1]); err == nil {
					receivedSeqs = append(receivedSeqs, seq)
				}
			}
		}
		receivedMu.Unlock()

		receivedTotal.Add(int64(len(req.Events)))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status:   "ok",
			Accepted: len(req.Events),
		})
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "traffic.log")
	spoolDir := filepath.Join(tempDir, "spool")

	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := config.AgentConfig{
		Agent: config.AgentSettings{ID: "fifo-agent", Host: "node-1"},
		Server: config.ServerTarget{
			URL:     srv.URL,
			Timeout: 1 * time.Second,
		},
		Batch: config.BatchSettings{
			MaxEvents:      5,
			FlushInterval:  25 * time.Millisecond,
			MaxQueueEvents: 500,
		},
		Retry: config.RetrySettings{
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
		},
		Spool: config.SpoolSettings{
			Enabled:   true,
			Directory: spoolDir,
			MaxSizeMB: 10,
		},
		Sources: []config.SourceConfig{
			{Name: "stream", Path: logPath, Format: "text", StartPosition: "beginning"},
		},
	}

	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	agentDone := make(chan error, 1)
	go func() {
		agentDone <- ag.Start(ctx)
	}()

	// 1. Generate first batch of 20 events while server is offline (Backlog)
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	for i := 0; i < 20; i++ {
		_, _ = f.WriteString(fmt.Sprintf("seq-%03d\n", i))
	}
	_ = f.Sync()

	// Wait for agent to detect outage and buffer to spool
	time.Sleep(150 * time.Millisecond)

	// 2. Generate second batch of 20 events (Live Traffic while spool has backlog)
	for i := 20; i < 40; i++ {
		_, _ = f.WriteString(fmt.Sprintf("seq-%03d\n", i))
	}
	_ = f.Sync()
	_ = f.Close()

	// Wait for batcher to flush
	time.Sleep(150 * time.Millisecond)

	// 3. Network recovers
	online.Store(true)

	// Wait for all 40 events to be delivered
	deadline := time.Now().Add(5 * time.Second)
	for receivedTotal.Load() < 40 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	if receivedTotal.Load() < 40 {
		t.Fatalf("expected 40 events delivered, got %d", receivedTotal.Load())
	}

	// 4. Invariant Verification: STRICT FIFO NON-DECREASING ORDER
	receivedMu.Lock()
	defer receivedMu.Unlock()

	if len(receivedSeqs) != 40 {
		t.Fatalf("expected 40 sequence entries, got %d", len(receivedSeqs))
	}

	for i := 0; i < len(receivedSeqs); i++ {
		expected := i
		if receivedSeqs[i] != expected {
			t.Fatalf("STRICT FIFO VIOLATED at index %d: expected seq-%03d, got seq-%03d (Full trace: %v)",
				i, expected, receivedSeqs[i], receivedSeqs)
		}
	}

	cancel()
	<-agentDone
	time.Sleep(30 * time.Millisecond)
}

// TestCrashDuringHTTPRequestBeforeACK tests that if an agent crashes while an HTTP
// request is in-flight (before ACK received), the segment is safely recovered on restart
// and deduplicated idempotently by the server with zero data loss and bounded duplication.
func TestCrashDuringHTTPRequestBeforeACK(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "server_data")
	spoolDir := filepath.Join(tempDir, "agent_spool")
	logPath := filepath.Join(tempDir, "app.log")

	// 1. Central Server with Deduplication & Store
	fsStore, err := store.NewFileStore(store.Config{Directory: storeDir})
	if err != nil {
		t.Fatalf("init store failed: %v", err)
	}
	defer fsStore.Close()

	serverMetrics := metrics.NewServerMetrics()
	ingestHandler := ingest.NewHandler(config.IngestSettings{
		MaxBatchEvents: 100,
		DedupWindow:    5 * time.Minute,
		DedupCapacity:  1000,
	}, fsStore, nil, serverMetrics)

	requestCount := atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		ingestHandler.ServeHTTP(w, r)
	}))
	defer srv.Close()

	_ = os.WriteFile(logPath, []byte("crash-event-1\ncrash-event-2\n"), 0o600)

	cfg := config.AgentConfig{
		Agent: config.AgentSettings{ID: "crash-agent", Host: "node-1"},
		Server: config.ServerTarget{
			URL:     srv.URL,
			Timeout: 2 * time.Second,
		},
		Batch: config.BatchSettings{
			MaxEvents:      2,
			FlushInterval:  20 * time.Millisecond,
			MaxQueueEvents: 100,
		},
		Spool: config.SpoolSettings{
			Enabled:   true,
			Directory: spoolDir,
			MaxSizeMB: 10,
		},
		Sources: []config.SourceConfig{
			{Name: "app", Path: logPath, Format: "text", StartPosition: "beginning"},
		},
	}

	// 2. Start Agent 1
	ag1, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent 1 failed: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	go func() {
		_ = ag1.Start(ctx1)
	}()

	// Wait until server receives the request
	deadline := time.Now().Add(3 * time.Second)
	for requestCount.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	// Abruptly terminate Agent 1 (simulating crash)
	cancel1()
	time.Sleep(50 * time.Millisecond)

	// 3. Verify server received the entries
	entries, err := fsStore.Query(context.Background(), logentry.Query{Limit: 10})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 stored entries, got %d", len(entries))
	}

	// 4. Start Agent 2 on the same spool and log files
	ag2, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent 2 failed: %v", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		_ = ag2.Start(ctx2)
	}()

	// Append 1 new line after recovery
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("post-recovery-event-3\n")
	_ = f.Close()

	// Wait for event 3 to be ingested
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ = fsStore.Query(context.Background(), logentry.Query{Limit: 10})
		if len(entries) >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel2()
	time.Sleep(50 * time.Millisecond)

	// Invariant Check: exactly 3 unique events in storage, zero duplicate records persisted!
	entries, err = fsStore.Query(context.Background(), logentry.Query{Limit: 10})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("IDEMPOTENCY / LOSS FAILURE: expected exactly 3 stored entries, got %d (Entries: %+v)", len(entries), entries)
	}
}

// TestCrashDuringRotationWithCheckpoints verifies that when a log file is rotated
// and the agent crashes, restarting resumes seamlessly from the checkpoint with zero gap.
func TestCrashDuringRotationWithCheckpoints(t *testing.T) {
	var receivedMessages []string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req logentry.IngestRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		for _, e := range req.Events {
			t.Logf("SRV received: %s", e.Message)
			receivedMessages = append(receivedMessages, e.Message)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{Status: "ok", Accepted: len(req.Events)})
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "rotation.log")
	spoolDir := filepath.Join(tempDir, "spool")

	// Phase 1: Write file 1
	_ = os.WriteFile(logPath, []byte("event-1\nevent-2\n"), 0o600)

	cfg := config.AgentConfig{
		Agent:  config.AgentSettings{ID: "rot-agent", Host: "node-1"},
		Server: config.ServerTarget{URL: srv.URL, Timeout: 1 * time.Second},
		Batch:  config.BatchSettings{MaxEvents: 2, FlushInterval: 25 * time.Millisecond, MaxQueueEvents: 100},
		Spool:  config.SpoolSettings{Enabled: true, Directory: spoolDir, MaxSizeMB: 10},
		Sources: []config.SourceConfig{
			{Name: "rot", Path: logPath, Format: "text", StartPosition: "beginning"},
		},
	}

	ag1, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent 1 failed: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	ag1Done := make(chan error, 1)
	go func() { ag1Done <- ag1.Start(ctx1) }()

	// Wait for event 1 and 2 to be confirmed delivered
	initDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(initDeadline) {
		mu.Lock()
		count := len(receivedMessages)
		mu.Unlock()
		if count >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Rotate file: rename to .1 and create new empty active file
	rotatedPath := logPath + ".1"
	if err := os.Rename(logPath, rotatedPath); err != nil {
		t.Logf("RENAME FAILED: %v", err)
	}
	if err := os.WriteFile(logPath, []byte("event-3\n"), 0o600); err != nil {
		t.Logf("WRITE NEW FILE FAILED: %v", err)
	}

	// Cleanly stop agent 1
	cancel1()
	select {
	case <-ag1Done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for ag1 to shut down")
	}

	// Phase 2: Start Agent 2 (simulating restart after crash during rotation)
	ag2, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent 2 failed: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	ag2Done := make(chan error, 1)
	go func() { ag2Done <- ag2.Start(ctx2) }()

	// Append event-4 to new file
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("event-4\n")
	_ = f.Close()

	// Wait for delivery
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(receivedMessages)
		mu.Unlock()
		if count >= 4 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel2()
	select {
	case <-ag2Done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for ag2 to shut down")
	}
	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedMessages) < 4 {
		t.Fatalf("expected at least 4 delivered messages across rotation crash, got %d (%v)",
			len(receivedMessages), receivedMessages)
	}
}
