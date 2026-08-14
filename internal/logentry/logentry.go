// Package logentry defines the canonical domain model and serialization
// types for log events ingested and streamed within Barnacles.
package logentry

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrMissingHost is returned when a log entry is missing the host field.
	ErrMissingHost = errors.New("log entry missing host")
	// ErrMissingSource is returned when a log entry is missing the source field.
	ErrMissingSource = errors.New("log entry missing source")
	// ErrMissingMessage is returned when a log entry is missing the message field.
	ErrMissingMessage = errors.New("log entry missing message")
	// ErrMessageTooLarge is returned when a log message exceeds maximum allowed size.
	ErrMessageTooLarge = errors.New("log message exceeds maximum size")
)

const (
	// MaxMessageBytes is the default safety ceiling for an individual log message (1MB).
	MaxMessageBytes = 1024 * 1024
	// DefaultQueryLimit is the default result count limit for log queries.
	DefaultQueryLimit = 100
	// MaxQueryLimit is the hard upper bound for log queries.
	MaxQueryLimit = 10000
)

// LogEntry is the canonical representation of a normalized log event in Barnacles.
type LogEntry struct {
	ID        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	Host      string            `json:"host"`
	Source    string            `json:"source"`
	Level     string            `json:"level,omitempty"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// New creates a new LogEntry with a generated UUID and current UTC timestamp if omitted.
func New(host, source, level, message string, fields map[string]string) LogEntry {
	var copiedFields map[string]string
	if len(fields) > 0 {
		copiedFields = make(map[string]string, len(fields))
		for k, v := range fields {
			copiedFields[k] = v
		}
	}

	normLevel := strings.ToUpper(strings.TrimSpace(level))
	if normLevel == "" {
		normLevel = "INFO"
	}

	return LogEntry{
		ID:        uuid.NewString(),
		Timestamp: time.Now().UTC(),
		Host:      strings.TrimSpace(host),
		Source:    strings.TrimSpace(source),
		Level:     normLevel,
		Message:   message,
		Fields:    copiedFields,
	}
}

// Validate verifies that the LogEntry satisfies structural constraints and required fields.
func (e *LogEntry) Validate(maxMsgBytes int) error {
	if strings.TrimSpace(e.ID) == "" {
		e.ID = uuid.NewString()
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	if strings.TrimSpace(e.Host) == "" {
		return ErrMissingHost
	}
	if strings.TrimSpace(e.Source) == "" {
		return ErrMissingSource
	}
	if e.Message == "" {
		return ErrMissingMessage
	}

	limit := maxMsgBytes
	if limit <= 0 {
		limit = MaxMessageBytes
	}
	if len(e.Message) > limit {
		return ErrMessageTooLarge
	}

	if e.Level == "" {
		e.Level = "INFO"
	} else {
		e.Level = strings.ToUpper(strings.TrimSpace(e.Level))
	}

	return nil
}

// Matches evaluates if the LogEntry satisfies the given Query criteria.
func (e *LogEntry) Matches(q Query) bool {
	if q.Host != "" && !strings.EqualFold(e.Host, q.Host) {
		return false
	}
	if q.Source != "" && !strings.EqualFold(e.Source, q.Source) {
		return false
	}
	if q.Level != "" && !strings.EqualFold(e.Level, q.Level) {
		return false
	}
	if !q.StartTime.IsZero() && e.Timestamp.Before(q.StartTime) {
		return false
	}
	if !q.EndTime.IsZero() && e.Timestamp.After(q.EndTime) {
		return false
	}
	if q.Search != "" {
		lowerSearch := strings.ToLower(q.Search)
		lowerMsg := strings.ToLower(e.Message)
		if !strings.Contains(lowerMsg, lowerSearch) {
			matchField := false
			for k, v := range e.Fields {
				if strings.Contains(strings.ToLower(k), lowerSearch) || strings.Contains(strings.ToLower(v), lowerSearch) {
					matchField = true
					break
				}
			}
			if !matchField {
				return false
			}
		}
	}
	return true
}

// IngestRequest represents an HTTP batch payload sent from an agent to the central server.
type IngestRequest struct {
	AgentID string     `json:"agent_id"`
	Events  []LogEntry `json:"events"`
}

// IngestResponse represents the server's response to an IngestRequest.
type IngestResponse struct {
	Status     string   `json:"status"`
	Accepted   int      `json:"accepted"`
	Duplicates int      `json:"duplicates,omitempty"`
	Errors     []string `json:"errors,omitempty"`
}

// Query specifies filtering parameters for retrieving logs from the server store.
type Query struct {
	Host      string    `json:"host"`
	Source    string    `json:"source"`
	Level     string    `json:"level"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Search    string    `json:"search"`
	Limit     int       `json:"limit"`
}

// Normalize enforces reasonable bounds and defaults on a Query.
func (q *Query) Normalize() {
	if q.Limit <= 0 {
		q.Limit = DefaultQueryLimit
	}
	if q.Limit > MaxQueryLimit {
		q.Limit = MaxQueryLimit
	}
	q.Host = strings.TrimSpace(q.Host)
	q.Source = strings.TrimSpace(q.Source)
	q.Level = strings.ToUpper(strings.TrimSpace(q.Level))
	q.Search = strings.TrimSpace(q.Search)
}
