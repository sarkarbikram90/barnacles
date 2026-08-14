package parser

import (
	"strings"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

// TextParser parses unformatted text lines into structured LogEntries.
type TextParser struct {
	host        string
	source      string
	defaultTags map[string]string
}

// NewTextParser creates a new TextParser.
func NewTextParser(host, source string, defaultTags map[string]string) *TextParser {
	return &TextParser{
		host:        host,
		source:      source,
		defaultTags: defaultTags,
	}
}

// Parse converts a plain text line into a LogEntry, inferring the log level heuristically.
func (p *TextParser) Parse(line string) (logentry.LogEntry, error) {
	trimmed := strings.TrimRight(line, "\r\n")
	if len(trimmed) == 0 {
		return logentry.LogEntry{}, ErrEmptyLine
	}

	lvl := detectLevel(trimmed)
	entry := logentry.New(p.host, p.source, lvl, trimmed, p.defaultTags)
	entry.Timestamp = time.Now().UTC()
	return entry, nil
}
