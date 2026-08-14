// Package parser provides flexible, resilient log parsing implementations
// that extract structured log entries from plain text, JSON, and regex patterns.
package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

var (
	// ErrEmptyLine is returned when attempting to parse an empty string.
	ErrEmptyLine = errors.New("empty log line")
	// ErrNoMatch is returned when a line does not match the configured regexp pattern.
	ErrNoMatch = errors.New("log line does not match pattern")
)

// Parser defines the contract for parsing a raw log line into a LogEntry.
type Parser interface {
	Parse(line string) (logentry.LogEntry, error)
}

// Compile-time interface checks.
var (
	_ Parser = (*TextParser)(nil)
	_ Parser = (*JSONParser)(nil)
	_ Parser = (*RegexpParser)(nil)
	_ Parser = (*AutoParser)(nil)
)

// New creates a configured Parser based on format string ("text", "json", "regexp", "auto").
func New(format, pattern, host, source string, defaultTags map[string]string) (Parser, error) {
	norm := strings.ToLower(strings.TrimSpace(format))
	switch norm {
	case "text":
		return NewTextParser(host, source, defaultTags), nil
	case "json":
		return NewJSONParser(host, source, defaultTags), nil
	case "regexp":
		return NewRegexpParser(pattern, host, source, defaultTags)
	case "", "auto":
		return NewAutoParser(host, source, defaultTags), nil
	default:
		return nil, fmt.Errorf("unsupported parser format: %q", format)
	}
}

// Level keywords for heuristic detection in plain text logs.
var levelKeywords = []string{
	"EMERGENCY", "ALERT", "CRITICAL", "FATAL",
	"ERROR", "WARN", "WARNING", "INFO", "NOTICE",
	"DEBUG", "TRACE",
}

// detectLevel performs a fast case-insensitive scan for standard log levels.
func detectLevel(line string) string {
	upper := strings.ToUpper(line)
	for _, lvl := range levelKeywords {
		idx := strings.Index(upper, lvl)
		if idx != -1 {
			// Check word boundaries or bracket encodings like [ERROR] or (INFO)
			if lvl == "WARNING" {
				return "WARN"
			}
			return lvl
		}
	}
	return "INFO"
}
