package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
)

func TestAgentCollectionAndDelivery(t *testing.T) {
	var (
		receivedCount int64
		receivedLock  sync.Mutex
		receivedMsgs  []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req logentry.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		receivedLock.Lock()
		for _, e := range req.Events {
			receivedMsgs = append(receivedMsgs, e.Message)
		}
		receivedLock.Unlock()

		atomic.AddInt64(&receivedCount, int64(len(req.Events)))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status:   "ok",
			Accepted: len(req.Events),
		})
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "service.log")
	spoolDir := filepath.Join(tempDir, "spool")

	if err := os.WriteFile(logFile, []byte(""), 0o600); err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}

	cfg := config.AgentConfig{
		Agent: config.AgentSettings{
			ID:   "test-agent",
			Host: "localhost",
		},
		Server: config.ServerTarget{
			URL:     srv.URL,
			Timeout: 2 * time.Second,
		},
		Batch: config.BatchSettings{
			MaxEvents:      2,
			FlushInterval:  50 * time.Millisecond,
			MaxQueueEvents: 100,
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
			{
				Name:          "service-log",
				Path:          logFile,
				Format:        "text",
				StartPosition: "beginning",
			},
		},
	}

	m := metrics.NewAgentMetrics()
	ag, err := New(cfg, m)
	if err != nil {
		t.Fatalf("New(agent) failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	agentDone := make(chan error, 1)
	go func() {
		agentDone <- ag.Start(ctx)
	}()

	// Write lines to log file
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	_, _ = f.WriteString("log entry 1\nlog entry 2\nlog entry 3\n")
	_ = f.Close()

	// Wait for server to receive all 3 events
	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt64(&receivedCount) < 3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	if atomic.LoadInt64(&receivedCount) < 3 {
		t.Fatalf("timed out waiting for events, got %d", atomic.LoadInt64(&receivedCount))
	}

	receivedLock.Lock()
	if len(receivedMsgs) != 3 || receivedMsgs[0] != "log entry 1" {
		t.Errorf("unexpected messages: %v", receivedMsgs)
	}
	receivedLock.Unlock()

	// Graceful shutdown
	cancel()
	select {
	case err := <-agentDone:
		if err != nil {
			t.Fatalf("agent Start returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("agent shutdown timed out")
	}
}

func TestAgentSpoolAndDrainOnOutage(t *testing.T) {
	var (
		serverAvailable atomic.Bool
		receivedTotal   atomic.Int64
	)

	serverAvailable.Store(false) // Server down initially

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !serverAvailable.Load() {
			http.Error(w, "server unavailable", http.StatusServiceUnavailable)
			return
		}
		var req logentry.IngestRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		receivedTotal.Add(int64(len(req.Events)))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{Status: "ok", Accepted: len(req.Events)})
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "outage.log")
	spoolDir := filepath.Join(tempDir, "spool")

	_ = os.WriteFile(logFile, []byte(""), 0o600)

	cfg := config.AgentConfig{
		Agent: config.AgentSettings{ID: "outage-agent", Host: "host-1"},
		Server: config.ServerTarget{
			URL:     srv.URL,
			Timeout: 1 * time.Second,
		},
		Batch: config.BatchSettings{
			MaxEvents:      1,
			FlushInterval:  20 * time.Millisecond,
			MaxQueueEvents: 100,
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
			{Name: "app", Path: logFile, Format: "text", StartPosition: "beginning"},
		},
	}

	ag, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New(agent) failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	agentDone := make(chan error, 1)
	go func() {
		agentDone <- ag.Start(ctx)
	}()

	// Write logs while server is down
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("outage line 1\noutage line 2\n")
	_ = f.Close()

	// Wait for agent to spool logs
	time.Sleep(200 * time.Millisecond)

	// Now bring server online
	serverAvailable.Store(true)

	// Wait for spool to drain to server
	deadline := time.Now().Add(3 * time.Second)
	for receivedTotal.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	if receivedTotal.Load() < 2 {
		t.Fatalf("expected at least 2 events delivered after recovery, got %d", receivedTotal.Load())
	}

	cancel()
	select {
	case <-agentDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("shutdown timed out")
	}
}
