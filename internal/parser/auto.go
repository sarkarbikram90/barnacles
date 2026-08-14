package parser

import (
	"strings"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

// AutoParser automatically detects JSON log entries or falls back to TextParser.
type AutoParser struct {
	jsonParser *JSONParser
	textParser *TextParser
}

// NewAutoParser returns a new AutoParser.
func NewAutoParser(host, source string, defaultTags map[string]string) *AutoParser {
	return &AutoParser{
		jsonParser: NewJSONParser(host, source, defaultTags),
		textParser: NewTextParser(host, source, defaultTags),
	}
}

// Parse tries JSON parsing if the line looks like JSON, falling back safely to TextParser.
func (p *AutoParser) Parse(line string) (logentry.LogEntry, error) {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) == 0 {
		return logentry.LogEntry{}, ErrEmptyLine
	}

	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		if entry, err := p.jsonParser.Parse(trimmed); err == nil {
			return entry, nil
		}
	}

	return p.textParser.Parse(line)
}
