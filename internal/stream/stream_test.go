package stream

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
)

func TestRecentBuffer(t *testing.T) {
	buf := NewRecentBuffer(3)

	if buf.Len() != 0 {
		t.Fatalf("expected initial len 0, got %d", buf.Len())
	}

	buf.Add(logentry.New("h", "s", "INFO", "msg 1", nil))
	buf.Add(logentry.New("h", "s", "INFO", "msg 2", nil))
	if buf.Len() != 2 {
		t.Fatalf("expected len 2, got %d", buf.Len())
	}

	snap := buf.Snapshot(10)
	if len(snap) != 2 || snap[0].Message != "msg 1" || snap[1].Message != "msg 2" {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	// Add 2 more to test circular wrap
	buf.Add(logentry.New("h", "s", "INFO", "msg 3", nil))
	buf.Add(logentry.New("h", "s", "INFO", "msg 4", nil))

	if buf.Len() != 3 {
		t.Fatalf("expected max capacity 3, got %d", buf.Len())
	}

	snapWrapped := buf.Snapshot(10)
	if len(snapWrapped) != 3 || snapWrapped[0].Message != "msg 2" || snapWrapped[2].Message != "msg 4" {
		t.Fatalf("unexpected wrapped snapshot: %+v", snapWrapped)
	}
}

func TestHubWebSocketStreaming(t *testing.T) {
	m := metrics.NewServerMetrics()
	cfg := config.StreamSettings{
		RecentEvents:     100,
		MaxClients:       10,
		ClientBufferSize: 64,
		PingInterval:     time.Second,
		WriteDeadline:    time.Second,
	}
	recent := NewRecentBuffer(100)
	recent.Add(logentry.New("h1", "app", "INFO", "initial message", nil))

	hub := NewHub(cfg, recent, m)
	defer hub.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = hub.Upgrade(w, r)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v", err)
	}
	defer ws.Close()

	// Read initial recent_batch
	_, p, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read message failed: %v", err)
	}

	var initMsg Message
	if err := json.Unmarshal(p, &initMsg); err != nil {
		t.Fatalf("unmarshal init msg failed: %v", err)
	}
	if initMsg.Type != "recent_batch" {
		t.Errorf("expected type 'recent_batch', got %s", initMsg.Type)
	}

	// Broadcast live message
	hub.Broadcast([]logentry.LogEntry{
		logentry.New("h1", "app", "WARN", "live streamed event", nil),
	})

	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, liveData, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read live message failed: %v", err)
	}

	var liveMsg Message
	if err := json.Unmarshal(liveData, &liveMsg); err != nil {
		t.Fatalf("unmarshal live msg failed: %v", err)
	}
	if liveMsg.Type != "log" {
		t.Errorf("expected type 'log', got %s", liveMsg.Type)
	}
}

func TestHubSlowClientDisconnect(t *testing.T) {
	m := metrics.NewServerMetrics()
	cfg := config.StreamSettings{
		RecentEvents:     10,
		MaxClients:       10,
		ClientBufferSize: 2, // very small buffer
		PingInterval:     time.Second,
		WriteDeadline:    time.Second,
	}

	hub := NewHub(cfg, nil, m)
	defer hub.Close()

	// Create a dummy client without reading its channel
	dummyClient := &Client{
		hub:  hub,
		send: make(chan []byte, 1),
	}
	hub.Register(dummyClient)

	if hub.ClientCount() != 1 {
		t.Fatalf("expected 1 client, got %d", hub.ClientCount())
	}

	// Fill client channel
	dummyClient.send <- []byte("msg 1")

	// Broadcast more events -> should overflow dummyClient and drop it without blocking
	hub.Broadcast([]logentry.LogEntry{
		logentry.New("h", "s", "INFO", "e1", nil),
		logentry.New("h", "s", "INFO", "e2", nil),
	})

	if hub.ClientCount() != 0 {
		t.Fatalf("expected slow client to be disconnected, got client count %d", hub.ClientCount())
	}
}
