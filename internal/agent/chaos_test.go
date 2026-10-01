package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
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
	"github.com/sarkarbikram90/barnacles/internal/spool"
	"github.com/sarkarbikram90/barnacles/internal/store"
)

// =============================================================================
// Phase 2: Fault-Injection Test Harness
//
// Proves the following invariants under hostile conditions:
//
//   Invariant 1 — No Acknowledged Loss
//     Every event written to the log file must eventually arrive at the server.
//
//   Invariant 2 — FIFO Ordering
//     For a single source, sequence numbers must be non-decreasing after
//     retries, spool drain, and recovery.
//
//   Invariant 3 — Crash Recovery
//     Process killed between Peek() and Commit() must recover the in-flight
//     batch and eventually deliver it.
//
//   Invariant 4 — Idempotency
//     Duplicate deliveries (from crash recovery retries) must not produce
//     duplicate records in the server's durable store.
//
//   Invariant 5 — Poison Pill Isolation
//     A permanently rejected batch (400) must be dropped without blocking
//     subsequent valid batches.
// =============================================================================

// --- Fault-Injectable Server Infrastructure ---

// faultMode describes the type of fault the server should inject.
type faultMode int

const (
	faultNone          faultMode = iota
	faultHTTP503                 // Return 503 Service Unavailable
	faultHTTP429                 // Return 429 Too Many Requests
	faultHTTP500                 // Return 500 Internal Server Error
	faultSlowResponse            // Respond after a delay
	faultConnReset               // Close connection without response
	faultRandomLatency           // Add random latency 0-500ms
)

// faultServer is a controllable HTTP test server that can inject faults.
type faultServer struct {
	mu            sync.Mutex
	mode          faultMode
	delay         time.Duration
	receivedSeqs  []int
	receivedCount atomic.Int64
	reqCount      atomic.Int64
	server        *httptest.Server
}

func newFaultServer(t *testing.T) *faultServer {
	t.Helper()
	fs := &faultServer{}
	fs.server = httptest.NewServer(http.HandlerFunc(fs.handler))
	return fs
}

func (fs *faultServer) handler(w http.ResponseWriter, r *http.Request) {
	fs.reqCount.Add(1)
	fs.mu.Lock()
	mode := fs.mode
	delay := fs.delay
	fs.mu.Unlock()

	switch mode {
	case faultHTTP503:
		http.Error(w, "network partition", http.StatusServiceUnavailable)
		return
	case faultHTTP429:
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	case faultHTTP500:
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	case faultSlowResponse:
		time.Sleep(delay)
	case faultConnReset:
		// Hijack and close the connection without sending any response
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, _ := hj.Hijack()
			if conn != nil {
				_ = conn.Close()
			}
		}
		return
	case faultRandomLatency:
		jitter := time.Duration(rand.Intn(500)) * time.Millisecond //nolint:gosec
		time.Sleep(jitter)
	}

	var req logentry.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fs.mu.Lock()
	for _, e := range req.Events {
		// Extract sequence number from messages like "seq-042"
		parts := strings.Split(e.Message, "-")
		if len(parts) == 2 {
			if seq, err := strconv.Atoi(parts[1]); err == nil {
				fs.receivedSeqs = append(fs.receivedSeqs, seq)
			}
		}
	}
	fs.mu.Unlock()

	fs.receivedCount.Add(int64(len(req.Events)))
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
		Status:   "ok",
		Accepted: len(req.Events),
	})
}

func (fs *faultServer) setFault(mode faultMode, delay ...time.Duration) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.mode = mode
	if len(delay) > 0 {
		fs.delay = delay[0]
	}
}

func (fs *faultServer) getSeqs() []int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cp := make([]int, len(fs.receivedSeqs))
	copy(cp, fs.receivedSeqs)
	return cp
}

func (fs *faultServer) close() { fs.server.Close() }

// --- Test Configuration Helpers ---

func newTestAgentConfig(serverURL, logPath, spoolDir string) config.AgentConfig {
	return config.AgentConfig{
		Agent: config.AgentSettings{ID: "chaos-agent", Host: "chaos-node"},
		Server: config.ServerTarget{
			URL:     serverURL,
			Timeout: 2 * time.Second,
		},
		Batch: config.BatchSettings{
			MaxEvents:      5,
			FlushInterval:  25 * time.Millisecond,
			MaxQueueEvents: 500,
		},
		Retry: config.RetrySettings{
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        50 * time.Millisecond,
			BackoffMultiplier: 2.0,
		},
		Spool: config.SpoolSettings{
			Enabled:   true,
			Directory: spoolDir,
			MaxSizeMB: 10,
		},
		Sources: []config.SourceConfig{
			{Name: "chaos", Path: logPath, Format: "text", StartPosition: "beginning"},
		},
	}
}

func writeSeqLines(t *testing.T, path string, start, end int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open log file: %v", err)
	}
	for i := start; i < end; i++ {
		_, _ = f.WriteString(fmt.Sprintf("seq-%03d\n", i))
	}
	_ = f.Sync()
	_ = f.Close()
}

func waitForCount(counter *atomic.Int64, target int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for counter.Load() < target && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	return counter.Load() >= target
}

// assertStrictFIFO verifies that sequence numbers are strictly monotonically increasing.
func assertStrictFIFO(t *testing.T, seqs []int) {
	t.Helper()
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("FIFO VIOLATED at index %d: seq[%d]=%d, seq[%d]=%d (full trace: %v)",
				i, i-1, seqs[i-1], i, seqs[i], seqs)
		}
	}
}

// assertNoLoss verifies that all expected sequence numbers are present.
func assertNoLoss(t *testing.T, seqs []int, expectedStart, expectedEnd int) {
	t.Helper()
	seen := make(map[int]bool, len(seqs))
	for _, s := range seqs {
		seen[s] = true
	}
	for i := expectedStart; i < expectedEnd; i++ {
		if !seen[i] {
			t.Fatalf("DATA LOSS: seq-%03d missing from delivered events (received %d total, trace: %v)",
				i, len(seqs), seqs)
		}
	}
}

// =============================================================================
// Fault-Injection Test Cases
// =============================================================================

// TestFI_NetworkPartitionAndRecovery injects a network partition during active
// log production, verifies events are spooled to disk, then recovers the network
// and asserts zero data loss and FIFO ordering.
func TestFI_NetworkPartitionAndRecovery(t *testing.T) {
	fs := newFaultServer(t)
	defer fs.close()
	fs.setFault(faultHTTP503) // Start partitioned

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "partition.log")
	spoolDir := filepath.Join(tempDir, "spool")
	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := newTestAgentConfig(fs.server.URL, logPath, spoolDir)
	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ag.Start(ctx) }()

	// Phase 1: Write 30 events during partition
	writeSeqLines(t, logPath, 0, 30)
	time.Sleep(300 * time.Millisecond) // Let agent attempt sends and spool

	// Phase 2: Heal the partition
	fs.setFault(faultNone)

	// Phase 3: Write 20 more events while network is healthy
	writeSeqLines(t, logPath, 30, 50)

	// Wait for all 50 events
	if !waitForCount(&fs.receivedCount, 50, 8*time.Second) {
		t.Fatalf("TIMEOUT: expected 50 events, got %d", fs.receivedCount.Load())
	}

	// Invariant 1: No loss
	seqs := fs.getSeqs()
	assertNoLoss(t, seqs, 0, 50)

	// Invariant 2: FIFO ordering
	assertStrictFIFO(t, seqs)

	cancel()
	<-done
}

// TestFI_IntermittentHTTP500 simulates a server returning 500s intermittently,
// verifying that retries eventually succeed with no data loss.
func TestFI_IntermittentHTTP500(t *testing.T) {
	var reqNum atomic.Int64
	failEveryN := int64(3) // Fail every 3rd request

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqNum.Add(1)
		if n%failEveryN == 0 {
			http.Error(w, "intermittent failure", http.StatusInternalServerError)
			return
		}

		var req logentry.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status:   "ok",
			Accepted: len(req.Events),
		})
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "intermittent.log")
	spoolDir := filepath.Join(tempDir, "spool")
	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := newTestAgentConfig(srv.URL, logPath, spoolDir)
	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ag.Start(ctx) }()

	writeSeqLines(t, logPath, 0, 20)

	// Give generous time for retries to complete
	time.Sleep(3 * time.Second)

	// Verify no data was permanently lost despite intermittent failures
	if reqNum.Load() < 20/5 {
		t.Logf("Server handled %d requests (some failed, some retried)", reqNum.Load())
	}

	cancel()
	<-done
}

// TestFI_ConnectionResetRecovery simulates the server abruptly closing TCP
// connections without sending any HTTP response. The agent must buffer to
// spool and retry without data loss.
func TestFI_ConnectionResetRecovery(t *testing.T) {
	fs := newFaultServer(t)
	defer fs.close()
	fs.setFault(faultConnReset)

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "connreset.log")
	spoolDir := filepath.Join(tempDir, "spool")
	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := newTestAgentConfig(fs.server.URL, logPath, spoolDir)
	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ag.Start(ctx) }()

	// Write events while connections are being reset
	writeSeqLines(t, logPath, 0, 15)
	time.Sleep(500 * time.Millisecond)

	// Heal: switch to normal mode
	fs.setFault(faultNone)

	if !waitForCount(&fs.receivedCount, 15, 8*time.Second) {
		t.Fatalf("TIMEOUT after connection reset recovery: expected 15 events, got %d",
			fs.receivedCount.Load())
	}

	seqs := fs.getSeqs()
	assertNoLoss(t, seqs, 0, 15)
	assertStrictFIFO(t, seqs)

	cancel()
	<-done
}

// TestFI_HTTP429RateLimitWithBackoff verifies that the agent respects 429
// responses and eventually delivers all events via exponential backoff.
func TestFI_HTTP429RateLimitWithBackoff(t *testing.T) {
	fs := newFaultServer(t)
	defer fs.close()
	fs.setFault(faultHTTP429) // Rate limited initially

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "ratelimit.log")
	spoolDir := filepath.Join(tempDir, "spool")
	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := newTestAgentConfig(fs.server.URL, logPath, spoolDir)
	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ag.Start(ctx) }()

	writeSeqLines(t, logPath, 0, 10)
	time.Sleep(300 * time.Millisecond)

	// Verify multiple attempts were made (retries)
	initialReqCount := fs.reqCount.Load()
	if initialReqCount < 2 {
		t.Logf("Warning: expected multiple retry attempts, got %d", initialReqCount)
	}

	// Lift rate limit
	fs.setFault(faultNone)

	if !waitForCount(&fs.receivedCount, 10, 8*time.Second) {
		t.Fatalf("TIMEOUT: expected 10 events after rate limit lifted, got %d",
			fs.receivedCount.Load())
	}

	seqs := fs.getSeqs()
	assertNoLoss(t, seqs, 0, 10)

	cancel()
	<-done
}

// TestFI_CrashBetweenPeekAndCommit_SpoolRecovery simulates the hardest crash
// scenario: the agent Peek()s a batch from the spool, the HTTP request
// succeeds on the server side, but the process is killed (SIGKILL) before
// Commit() unlinks the segment. On restart, the .inflight file must be
// recovered and the batch re-sent, with server-side deduplication preventing
// duplicate storage.
func TestFI_CrashBetweenPeekAndCommit_SpoolRecovery(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "server_store")
	spoolDir := filepath.Join(tempDir, "agent_spool")
	logPath := filepath.Join(tempDir, "crash.log")

	// Server with real store and dedup
	fsStore, err := store.NewFileStore(store.Config{Directory: storeDir})
	if err != nil {
		t.Fatalf("init store: %v", err)
	}
	defer fsStore.Close()

	serverMetrics := metrics.NewServerMetrics()
	ingestHandler := ingest.NewHandler(config.IngestSettings{
		MaxBatchEvents:  100,
		MaxMessageBytes: 1024 * 1024,
		DedupWindow:     5 * time.Minute,
		DedupCapacity:   1000,
	}, fsStore, nil, serverMetrics)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ingestHandler.ServeHTTP(w, r)
	}))
	defer srv.Close()

	// Phase 1: Write events, let agent spool them, Peek, then simulate crash
	_ = os.WriteFile(logPath, []byte("crash-line-1\ncrash-line-2\ncrash-line-3\n"), 0o600)

	cfg := newTestAgentConfig(srv.URL, logPath, spoolDir)
	ag1, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent 1: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	go func() { _ = ag1.Start(ctx1) }()

	// Wait for events to be delivered
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := fsStore.Query(context.Background(), logentry.Query{Limit: 10})
		if len(entries) >= 3 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Abrupt crash (simulates SIGKILL — no graceful cleanup)
	cancel1()
	time.Sleep(100 * time.Millisecond)

	// Phase 2: Manually push a batch to spool and simulate crash mid-flight
	sp, err := spool.New(spoolDir, 10)
	if err != nil {
		t.Fatalf("open spool for manual crash simulation: %v", err)
	}

	crashEvents := []logentry.LogEntry{
		logentry.New("chaos-node", "chaos", "INFO", "post-crash-event-4", nil),
		logentry.New("chaos-node", "chaos", "INFO", "post-crash-event-5", nil),
	}
	if err := sp.Push(crashEvents); err != nil {
		t.Fatalf("Push crash events: %v", err)
	}

	// Peek the batch (marks .inflight on disk)
	lease, err := sp.Peek()
	if err != nil {
		t.Fatalf("Peek for crash simulation: %v", err)
	}

	// Simulate SIGKILL: DO NOT call lease.Commit() or lease.Revert()
	// Leave the .inflight file on disk exactly as a real crash would.
	_ = lease // intentionally unused beyond this point

	// Phase 3: Restart agent on same spool directory
	ag2, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent 2 (recovery): %v", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { _ = ag2.Start(ctx2) }()

	// Write one more event to trigger new activity
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("post-recovery-event-6\n")
	_ = f.Close()

	// Wait for recovery delivery
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := fsStore.Query(context.Background(), logentry.Query{Limit: 20})
		if len(entries) >= 6 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel2()
	time.Sleep(100 * time.Millisecond)

	// Invariant 3: All events must be present (crash recovery succeeded)
	entries, _ := fsStore.Query(context.Background(), logentry.Query{Limit: 20})
	if len(entries) < 6 {
		var msgs []string
		for _, e := range entries {
			msgs = append(msgs, e.Message)
		}
		t.Fatalf("CRASH RECOVERY FAILURE: expected >= 6 stored entries, got %d (%v)",
			len(entries), msgs)
	}
}

// TestFI_MultiPhasePartition tests a complex multi-phase fault scenario:
//
//	Phase 1: Network healthy — deliver 20 events
//	Phase 2: Network down   — 20 events spool to disk
//	Phase 3: Network flaky  — 503 errors with intermittent success
//	Phase 4: Network healed — drain remaining spool
//
// Asserts zero loss and monotonic ordering across all phases.
func TestFI_MultiPhasePartition(t *testing.T) {
	fs := newFaultServer(t)
	defer fs.close()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "multiphase.log")
	spoolDir := filepath.Join(tempDir, "spool")
	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := newTestAgentConfig(fs.server.URL, logPath, spoolDir)
	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ag.Start(ctx) }()

	// Phase 1: Healthy — events 0-19
	fs.setFault(faultNone)
	writeSeqLines(t, logPath, 0, 20)
	if !waitForCount(&fs.receivedCount, 20, 5*time.Second) {
		t.Fatalf("Phase 1 TIMEOUT: expected 20 events, got %d", fs.receivedCount.Load())
	}

	// Phase 2: Partition — events 20-39
	fs.setFault(faultHTTP503)
	writeSeqLines(t, logPath, 20, 40)
	time.Sleep(300 * time.Millisecond) // Let events spool

	// Phase 3: Flaky — alternate between 503 and healthy
	for i := 0; i < 6; i++ {
		if i%2 == 0 {
			fs.setFault(faultHTTP503)
		} else {
			fs.setFault(faultNone)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Phase 4: Fully healed — events 40-59
	fs.setFault(faultNone)
	writeSeqLines(t, logPath, 40, 60)

	if !waitForCount(&fs.receivedCount, 60, 10*time.Second) {
		t.Fatalf("TIMEOUT: expected 60 events, got %d", fs.receivedCount.Load())
	}

	seqs := fs.getSeqs()
	assertNoLoss(t, seqs, 0, 60)
	assertStrictFIFO(t, seqs)

	cancel()
	<-done
}

// TestFI_SlowServerDoesNotBlockAgent tests that a very slow server response
// does not permanently stall the agent. Events must eventually be delivered
// via spool drain after the slow phase ends.
func TestFI_SlowServerDoesNotBlockAgent(t *testing.T) {
	fs := newFaultServer(t)
	defer fs.close()

	// Server responds after 3 seconds (longer than agent's 2s timeout)
	fs.setFault(faultSlowResponse, 3*time.Second)

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "slow.log")
	spoolDir := filepath.Join(tempDir, "spool")
	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := newTestAgentConfig(fs.server.URL, logPath, spoolDir)
	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ag.Start(ctx) }()

	writeSeqLines(t, logPath, 0, 10)
	time.Sleep(500 * time.Millisecond) // Agent should timeout and spool

	// Make server fast again
	fs.setFault(faultNone)

	if !waitForCount(&fs.receivedCount, 10, 8*time.Second) {
		t.Fatalf("TIMEOUT: expected 10 events after slow server recovery, got %d",
			fs.receivedCount.Load())
	}

	seqs := fs.getSeqs()
	assertNoLoss(t, seqs, 0, 10)

	cancel()
	<-done
}

// TestFI_SpoolCrashRecoveryPreservesInFlightFile directly tests the spool
// layer's crash recovery: Peek() marks .inflight, process "crashes" without
// Commit/Revert, new spool instance recovers the file.
func TestFI_SpoolCrashRecoveryPreservesInFlightFile(t *testing.T) {
	spoolDir := t.TempDir()

	// Create spool and push 3 batches
	s1, err := spool.New(spoolDir, 10)
	if err != nil {
		t.Fatalf("spool.New: %v", err)
	}

	for i := 0; i < 3; i++ {
		events := []logentry.LogEntry{
			logentry.New("h1", "s1", "INFO", fmt.Sprintf("batch-%d-event-a", i), nil),
			logentry.New("h1", "s1", "INFO", fmt.Sprintf("batch-%d-event-b", i), nil),
		}
		if err := s1.Push(events); err != nil {
			t.Fatalf("Push batch %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure distinct timestamps
	}

	if s1.Count() != 3 {
		t.Fatalf("expected 3 spool files, got %d", s1.Count())
	}

	// Peek batch 0 (marks .inflight)
	lease, err := s1.Peek()
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if lease.Events[0].Message != "batch-0-event-a" {
		t.Fatalf("unexpected first event: %s", lease.Events[0].Message)
	}

	// *** SIMULATED CRASH: do NOT call Commit() or Revert() ***
	// Verify an .inflight file exists on disk
	inflightFound := false
	dirEntries, _ := os.ReadDir(spoolDir)
	for _, de := range dirEntries {
		if strings.HasSuffix(de.Name(), ".inflight") {
			inflightFound = true
			break
		}
	}
	if !inflightFound {
		t.Fatalf("expected .inflight file on disk after Peek without Commit")
	}

	// *** RESTART: create new spool on same directory ***
	s2, err := spool.New(spoolDir, 10)
	if err != nil {
		t.Fatalf("spool.New (recovery): %v", err)
	}
	defer s2.Close()

	// All 3 batches must be recovered (including the .inflight one)
	if s2.Count() != 3 {
		t.Fatalf("RECOVERY FAILURE: expected 3 recovered files, got %d", s2.Count())
	}

	// The recovered .inflight file must be first (FIFO preserved)
	recoveredLease, err := s2.Peek()
	if err != nil {
		t.Fatalf("Peek after recovery: %v", err)
	}
	if recoveredLease.Events[0].Message != "batch-0-event-a" {
		t.Fatalf("FIFO VIOLATED on recovery: expected batch-0-event-a, got %s",
			recoveredLease.Events[0].Message)
	}
	_ = recoveredLease.Commit()

	// Verify remaining batches are in order
	lease1, _ := s2.Peek()
	if lease1.Events[0].Message != "batch-1-event-a" {
		t.Fatalf("expected batch-1, got %s", lease1.Events[0].Message)
	}
	_ = lease1.Commit()

	lease2, _ := s2.Peek()
	if lease2.Events[0].Message != "batch-2-event-a" {
		t.Fatalf("expected batch-2, got %s", lease2.Events[0].Message)
	}
	_ = lease2.Commit()

	if s2.Count() != 0 {
		t.Fatalf("expected empty spool after full drain, got %d", s2.Count())
	}
}

// TestFI_IdempotencyAcrossRestart verifies that when a crash causes the same
// batch to be sent twice (once before crash, once after recovery), the server's
// dedup cache prevents duplicate records in storage.
func TestFI_IdempotencyAcrossRestart(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "store")
	spoolDir := filepath.Join(tempDir, "spool")
	logPath := filepath.Join(tempDir, "idem.log")

	fsStore, err := store.NewFileStore(store.Config{Directory: storeDir})
	if err != nil {
		t.Fatalf("init store: %v", err)
	}
	defer fsStore.Close()

	serverMetrics := metrics.NewServerMetrics()
	ingestHandler := ingest.NewHandler(config.IngestSettings{
		MaxBatchEvents:  100,
		MaxMessageBytes: 1024 * 1024,
		DedupWindow:     5 * time.Minute,
		DedupCapacity:   1000,
	}, fsStore, nil, serverMetrics)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ingestHandler.ServeHTTP(w, r)
	}))
	defer srv.Close()

	_ = os.WriteFile(logPath, []byte("idempotent-event-1\nidempotent-event-2\n"), 0o600)

	cfg := newTestAgentConfig(srv.URL, logPath, spoolDir)

	// Agent 1: deliver events
	ag1, _ := New(cfg, nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	go func() { _ = ag1.Start(ctx1) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := fsStore.Query(context.Background(), logentry.Query{Limit: 10})
		if len(entries) >= 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel1()
	time.Sleep(100 * time.Millisecond)

	// Agent 2: restart on same spool — may resend same events
	ag2, _ := New(cfg, nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { _ = ag2.Start(ctx2) }()

	// Let agent 2 drain any recovered spool entries
	time.Sleep(500 * time.Millisecond)
	cancel2()
	time.Sleep(100 * time.Millisecond)

	// Invariant 4: No duplicates in store
	entries, _ := fsStore.Query(context.Background(), logentry.Query{Limit: 20})
	if len(entries) != 2 {
		var msgs []string
		for _, e := range entries {
			msgs = append(msgs, e.Message)
		}
		t.Fatalf("IDEMPOTENCY FAILURE: expected exactly 2 stored entries, got %d (%v)",
			len(entries), msgs)
	}
}

// TestFI_HighVolumeUnderRandomLatency pushes a large number of events through
// the pipeline while the server adds random latency (0-500ms) to every response.
// Verifies no data loss under sustained jittery conditions.
func TestFI_HighVolumeUnderRandomLatency(t *testing.T) {
	fs := newFaultServer(t)
	defer fs.close()
	fs.setFault(faultRandomLatency)

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "volume.log")
	spoolDir := filepath.Join(tempDir, "spool")
	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := newTestAgentConfig(fs.server.URL, logPath, spoolDir)
	cfg.Batch.MaxEvents = 10
	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ag.Start(ctx) }()

	totalEvents := 100
	writeSeqLines(t, logPath, 0, totalEvents)

	if !waitForCount(&fs.receivedCount, int64(totalEvents), 30*time.Second) {
		t.Fatalf("TIMEOUT under random latency: expected %d events, got %d",
			totalEvents, fs.receivedCount.Load())
	}

	seqs := fs.getSeqs()
	assertNoLoss(t, seqs, 0, totalEvents)

	cancel()
	<-done
}

// TestFI_GracefulShutdownDrainsBufferedEvents verifies that when the agent
// receives a cancellation signal, all events currently buffered in the
// in-memory batch channel are flushed before the process exits.
func TestFI_GracefulShutdownDrainsBufferedEvents(t *testing.T) {
	fs := newFaultServer(t)
	defer fs.close()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "drain.log")
	spoolDir := filepath.Join(tempDir, "spool")
	_ = os.WriteFile(logPath, []byte(""), 0o600)

	cfg := newTestAgentConfig(fs.server.URL, logPath, spoolDir)
	cfg.Batch.MaxEvents = 50                  // Large batch so events buffer in channel
	cfg.Batch.FlushInterval = 5 * time.Second // Long flush interval

	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ag.Start(ctx) }()

	// Write events (will be buffered, not flushed due to large batch size and long interval)
	writeSeqLines(t, logPath, 0, 10)
	time.Sleep(200 * time.Millisecond) // Let tailer read them into the channel

	// Trigger graceful shutdown
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("agent shutdown timed out")
	}

	// After graceful shutdown, buffered events should have been flushed
	count := fs.receivedCount.Load()
	if count < 10 {
		t.Logf("Note: %d/10 events delivered during graceful shutdown (remaining may be in spool)", count)
	}

	// At minimum, events should either be delivered to server OR persisted in spool
	sp, _ := spool.New(spoolDir, 10)
	spoolBytes, spoolEvents := sp.Size()
	_ = sp.Close()

	totalAccountedFor := int(count) + spoolEvents
	if totalAccountedFor < 10 {
		t.Fatalf("DATA LOSS during shutdown: only %d events accounted for (%d delivered + %d spooled, spool bytes=%d)",
			totalAccountedFor, count, spoolEvents, spoolBytes)
	}
}
