package sender

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/protocol"
)

func decodeBody(r *http.Request) (logentry.IngestRequest, error) {
	var bodyReader io.Reader = r.Body
	switch r.Header.Get("Content-Encoding") {
	case "zstd":
		zr, err := zstd.NewReader(r.Body)
		if err != nil {
			return logentry.IngestRequest{}, err
		}
		defer zr.Close()
		bodyReader = zr
	case "gzip":
		gr, err := gzip.NewReader(r.Body)
		if err != nil {
			return logentry.IngestRequest{}, err
		}
		defer gr.Close()
		bodyReader = gr
	}

	var req logentry.IngestRequest
	err := json.NewDecoder(bodyReader).Decode(&req)
	return req, err
}

func TestSenderSuccessWithToken(t *testing.T) {
	var receivedToken string
	var receivedReq logentry.IngestRequest
	var receivedEncoding string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedToken = r.Header.Get("Authorization")
		receivedEncoding = r.Header.Get("Content-Encoding")
		req, err := decodeBody(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		receivedReq = req
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status:   "ok",
			Accepted: len(receivedReq.Events),
		})
	}))
	defer srv.Close()

	snd, err := New(Config{
		URL:         srv.URL,
		Token:       "secret-token-123",
		Compression: "zstd",
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	events := []logentry.LogEntry{
		logentry.New("host1", "syslog", "INFO", "hello world", nil),
	}

	resp, err := snd.Send(context.Background(), "agent-1", events)
	if err != nil {
		t.Fatalf("Send() failed: %v", err)
	}

	if resp.Accepted != 1 || resp.Status != "ok" {
		t.Errorf("unexpected response: %+v", resp)
	}
	if receivedToken != "Bearer secret-token-123" {
		t.Errorf("unexpected token header: %s", receivedToken)
	}
	if receivedEncoding != "zstd" {
		t.Errorf("expected Content-Encoding zstd, got: %s", receivedEncoding)
	}
	if len(receivedReq.Events) != 1 || receivedReq.Events[0].Message != "hello world" {
		t.Errorf("unexpected received payload: %+v", receivedReq)
	}
}

func TestSenderCompressionModes(t *testing.T) {
	modes := []struct {
		compression      string
		expectedEncoding string
	}{
		{"zstd", "zstd"},
		{"gzip", "gzip"},
		{"none", ""},
	}

	for _, m := range modes {
		t.Run("mode_"+m.compression, func(t *testing.T) {
			var receivedEncoding string
			var receivedReq logentry.IngestRequest

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedEncoding = r.Header.Get("Content-Encoding")
				req, err := decodeBody(r)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				receivedReq = req
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
					Status:   "ok",
					Accepted: len(receivedReq.Events),
				})
			}))
			defer srv.Close()

			snd, err := New(Config{
				URL:         srv.URL,
				Compression: m.compression,
			})
			if err != nil {
				t.Fatalf("New failed: %v", err)
			}

			events := []logentry.LogEntry{
				logentry.New("node1", "app", "WARN", "compression test message", nil),
			}

			resp, err := snd.Send(context.Background(), "agent-c", events)
			if err != nil {
				t.Fatalf("Send failed: %v", err)
			}
			if resp.Accepted != 1 {
				t.Errorf("expected 1 accepted, got %d", resp.Accepted)
			}
			if receivedEncoding != m.expectedEncoding {
				t.Errorf("expected encoding %q, got %q", m.expectedEncoding, receivedEncoding)
			}
			if len(receivedReq.Events) != 1 || receivedReq.Events[0].Message != "compression test message" {
				t.Errorf("unexpected payload: %+v", receivedReq)
			}
		})
	}
}

func TestSenderRetryableErrors(t *testing.T) {
	statusCodes := []int{
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
	}

	for _, code := range statusCodes {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))

		snd, _ := New(Config{URL: srv.URL})
		_, err := snd.Send(context.Background(), "agent-1", []logentry.LogEntry{
			logentry.New("h", "s", "INFO", "m", nil),
		})
		srv.Close()

		if err == nil {
			t.Fatalf("expected error for status %d", code)
		}
		if !IsRetryable(err) {
			t.Errorf("status %d should be retryable, got: %v", code, err)
		}
	}
}

func TestSenderPermanentErrors(t *testing.T) {
	statusCodes := []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusUnprocessableEntity,
	}

	for _, code := range statusCodes {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))

		snd, _ := New(Config{URL: srv.URL})
		_, err := snd.Send(context.Background(), "agent-1", []logentry.LogEntry{
			logentry.New("h", "s", "INFO", "m", nil),
		})
		srv.Close()

		if err == nil {
			t.Fatalf("expected error for status %d", code)
		}
		if IsRetryable(err) {
			t.Errorf("status %d should be permanent (non-retryable), got: %v", code, err)
		}
		if !errors.Is(err, ErrPermanent) {
			t.Errorf("expected ErrPermanent, got %v", err)
		}
	}
}

func TestCalculateBackoff(t *testing.T) {
	initial := 100 * time.Millisecond
	max := 2 * time.Second

	for attempt := 0; attempt < 10; attempt++ {
		b := CalculateBackoff(attempt, initial, max, 2.0)
		if b < initial/2 {
			t.Errorf("backoff %v below min floor %v", b, initial/2)
		}
		if b > max {
			t.Errorf("backoff %v exceeded max %v", b, max)
		}
	}
}

func TestSenderProtobufFormat(t *testing.T) {
	var (
		receivedContentType string
		receivedAccept      string
		receivedReq         logentry.IngestRequest
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedContentType = r.Header.Get("Content-Type")
		receivedAccept = r.Header.Get("Accept")

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := protocol.UnmarshalIngestRequest(bodyBytes, &receivedReq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp := logentry.IngestResponse{
			Status:   "ok",
			Accepted: len(receivedReq.Events),
		}
		respBytes, _ := protocol.MarshalIngestResponse(&resp)
		w.Header().Set("Content-Type", protocol.ContentTypeProtobuf)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	defer srv.Close()

	snd, err := New(Config{
		URL:         srv.URL,
		Format:      "protobuf",
		Compression: "none",
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	events := []logentry.LogEntry{
		logentry.New("host1", "source1", "INFO", "hello proto", map[string]string{"k": "v"}),
	}

	resp, err := snd.Send(context.Background(), "agent-pb", events)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if resp.Accepted != 1 {
		t.Errorf("expected 1 accepted, got %d", resp.Accepted)
	}
	if receivedContentType != protocol.ContentTypeProtobuf {
		t.Errorf("expected Content-Type %s, got %s", protocol.ContentTypeProtobuf, receivedContentType)
	}
	if receivedAccept != protocol.ContentTypeProtobuf {
		t.Errorf("expected Accept %s, got %s", protocol.ContentTypeProtobuf, receivedAccept)
	}
	if receivedReq.AgentID != "agent-pb" {
		t.Errorf("expected AgentID 'agent-pb', got %s", receivedReq.AgentID)
	}
	if len(receivedReq.Events) != 1 || receivedReq.Events[0].Message != "hello proto" {
		t.Errorf("received events mismatch: %+v", receivedReq.Events)
	}
}
