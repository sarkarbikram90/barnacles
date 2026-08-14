package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
)

func TestServerEndToEnd(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "server_logs")

	cfg := config.DefaultServerConfig()
	cfg.Server.Address = "127.0.0.1:0" // Pick random available port
	cfg.Storage.Directory = storeDir
	cfg.Auth.Enabled = true
	cfg.Auth.Tokens = []string{"test-secret-token"}

	m := metrics.NewServerMetrics()
	srv, err := New(cfg, m)
	if err != nil {
		t.Fatalf("New(server) failed: %v", err)
	}

	testServer := httptest.NewServer(srv.httpServer.Handler)
	defer testServer.Close()

	// 1. Test Health endpoints
	resHealth, err := http.Get(testServer.URL + "/healthz")
	if err != nil || resHealth.StatusCode != http.StatusOK {
		t.Fatalf("health check failed: %v", err)
	}

	// 2. Test Ingest without auth (should fail with 401)
	rawBatch := `{"agent_id":"a1","events":[{"id":"e1","host":"h1","source":"s1","message":"hello"}]}`
	resNoAuth, err := http.Post(testServer.URL+"/api/v1/ingest", "application/json", strings.NewReader(rawBatch))
	if err != nil || resNoAuth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized without token, got status: %d", resNoAuth.StatusCode)
	}

	// 3. Connect WebSocket client with token
	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/ws?token=test-secret-token"
	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if resp != nil {
			t.Fatalf("websocket connection failed: %v, status=%d", err, resp.StatusCode)
		}
		t.Fatalf("websocket connection failed: %v", err)
	}
	defer wsConn.Close()

	// Read initial recent_batch frame sent on connection
	_ = wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, initMsg, err := wsConn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read initial message: %v", err)
	}
	if !strings.Contains(string(initMsg), "recent_batch") {
		t.Errorf("expected recent_batch initial frame, got: %s", string(initMsg))
	}

	// 4. Test Ingest with auth
	reqAuth, err := http.NewRequest(http.MethodPost, testServer.URL+"/api/v1/ingest", bytes.NewReader([]byte(rawBatch)))
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	reqAuth.Header.Set("Authorization", "Bearer test-secret-token")
	reqAuth.Header.Set("Content-Type", "application/json")

	resAuth, err := http.DefaultClient.Do(reqAuth)
	if err != nil || resAuth.StatusCode != http.StatusOK {
		t.Fatalf("ingest with token failed: status=%d", resAuth.StatusCode)
	}

	var ingestResp logentry.IngestResponse
	_ = json.NewDecoder(resAuth.Body).Decode(&ingestResp)
	if ingestResp.Accepted != 1 {
		t.Errorf("expected 1 accepted, got: %+v", ingestResp)
	}

	// 5. Verify WebSocket client received the live streamed event
	_ = wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, wsMsg, err := wsConn.ReadMessage()
	if err != nil {
		t.Fatalf("websocket read message failed: %v", err)
	}
	if !strings.Contains(string(wsMsg), "hello") {
		t.Errorf("websocket did not receive expected message, got: %s", string(wsMsg))
	}

	// 6. Test Query API
	queryReq, _ := http.NewRequest(http.MethodGet, testServer.URL+"/api/v1/logs?host=h1", nil)
	queryReq.Header.Set("Authorization", "Bearer test-secret-token")
	queryRes, err := http.DefaultClient.Do(queryReq)
	if err != nil || queryRes.StatusCode != http.StatusOK {
		t.Fatalf("query failed: status=%d", queryRes.StatusCode)
	}

	var queryData struct {
		Total int                 `json:"total"`
		Logs  []logentry.LogEntry `json:"logs"`
	}
	_ = json.NewDecoder(queryRes.Body).Decode(&queryData)
	if queryData.Total != 1 || queryData.Logs[0].Message != "hello" {
		t.Errorf("unexpected query result: %+v", queryData)
	}

	// 7. Test Sources API
	srcReq, _ := http.NewRequest(http.MethodGet, testServer.URL+"/api/v1/sources", nil)
	srcReq.Header.Set("Authorization", "Bearer test-secret-token")
	srcRes, _ := http.DefaultClient.Do(srcReq)
	var srcData struct {
		Sources []string `json:"sources"`
	}
	_ = json.NewDecoder(srcRes.Body).Decode(&srcData)
	if len(srcData.Sources) != 1 || srcData.Sources[0] != "s1" {
		t.Errorf("unexpected sources result: %+v", srcData)
	}
}

func TestServerGracefulLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	cfg := config.DefaultServerConfig()
	cfg.Server.Address = "127.0.0.1:0"
	cfg.Storage.Directory = filepath.Join(tempDir, "logs")

	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New(server) failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server Start returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("server shutdown timed out")
	}
}
