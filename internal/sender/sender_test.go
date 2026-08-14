package sender

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

func TestSenderSuccessWithToken(t *testing.T) {
	var receivedToken string
	var receivedReq logentry.IngestRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedToken = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logentry.IngestResponse{
			Status:   "ok",
			Accepted: len(receivedReq.Events),
		})
	}))
	defer srv.Close()

	snd, err := New(Config{
		URL:   srv.URL,
		Token: "secret-token-123",
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
	if len(receivedReq.Events) != 1 || receivedReq.Events[0].Message != "hello world" {
		t.Errorf("unexpected received payload: %+v", receivedReq)
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
