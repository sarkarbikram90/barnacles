// Package sender provides an HTTP client for transmitting batched log events
// to the central Barnacles server with authentication, timeouts, and backoff classification.
package sender

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

// ErrPermanent indicates a non-retryable client error (e.g. 400 Bad Request, 401 Unauthorized).
var ErrPermanent = errors.New("permanent non-retryable error")

// ErrTemporary indicates a retryable server or network error.
var ErrTemporary = errors.New("temporary retryable error")

// Sender sends batched log events to the central Barnacles server.
type Sender struct {
	targetURL  string
	token      string
	httpClient *http.Client
}

// Config defines connection parameters for the Sender.
type Config struct {
	URL                string
	Token              string
	Timeout            time.Duration
	InsecureSkipVerify bool
}

// New creates a new configured Sender.
func New(cfg Config) (*Sender, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("server URL cannot be empty")
	}

	parsedURL, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL: %w", err)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec
		},
	}

	ingestURL := strings.TrimRight(parsedURL.String(), "/")
	if !strings.HasSuffix(ingestURL, "/api/v1/ingest") {
		ingestURL += "/api/v1/ingest"
	}

	return &Sender{
		targetURL: ingestURL,
		token:     cfg.Token,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
	}, nil
}

// Send transmits an ingest batch to the central server.
func (s *Sender) Send(ctx context.Context, agentID string, events []logentry.LogEntry) (*logentry.IngestResponse, error) {
	if len(events) == 0 {
		return &logentry.IngestResponse{Status: "ok", Accepted: 0}, nil
	}

	reqPayload := logentry.IngestRequest{
		AgentID: agentID,
		Events:  events,
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal request payload: %v", ErrPermanent, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: create http request: %v", ErrPermanent, err)
	}

	req.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		// Network errors, timeouts, connection refused are retryable
		return nil, fmt.Errorf("%w: http post failed: %v", ErrTemporary, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))

	// Handle status codes
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
		var ingestResp logentry.IngestResponse
		if err := json.Unmarshal(respBody, &ingestResp); err != nil {
			return &logentry.IngestResponse{Status: "ok", Accepted: len(events)}, nil
		}
		return &ingestResp, nil
	}

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, fmt.Errorf("%w: server returned status %d: %s", ErrTemporary, resp.StatusCode, string(respBody))
	}

	// 4xx client errors (400, 401, 403, 404, etc.) are permanent
	return nil, fmt.Errorf("%w: client error %d: %s", ErrPermanent, resp.StatusCode, string(respBody))
}

// IsRetryable returns true if the given error is considered temporary and safe to retry.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) {
		return false
	}
	if errors.Is(err, ErrTemporary) {
		return true
	}
	// Fallback check for net.Error timeouts
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return false
}

// CalculateBackoff computes exponential backoff with full jitter.
func CalculateBackoff(attempt int, initial, max time.Duration, multiplier float64) time.Duration {
	if attempt <= 0 {
		attempt = 0
	}
	if multiplier <= 1.0 {
		multiplier = 2.0
	}
	if initial <= 0 {
		initial = 1 * time.Second
	}
	if max <= 0 {
		max = 30 * time.Second
	}

	dur := float64(initial)
	for i := 0; i < attempt; i++ {
		dur *= multiplier
		if dur >= float64(max) {
			dur = float64(max)
			break
		}
	}

	// Add jitter in range [0.5, 1.5]
	jitter := 0.5 + rand.Float64() //nolint:gosec
	jittered := time.Duration(dur * jitter)
	if jittered > max {
		jittered = max
	}
	if jittered < initial/2 {
		jittered = initial / 2
	}
	return jittered
}
