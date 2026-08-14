package parser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

// RegexpParser parses log lines using a regular expression with named capture groups.
type RegexpParser struct {
	re          *regexp.Regexp
	host        string
	source      string
	defaultTags map[string]string
}

// NewRegexpParser compiles a regular expression and returns a RegexpParser.
func NewRegexpParser(pattern, host, source string, defaultTags map[string]string) (*RegexpParser, error) {
	if strings.TrimSpace(pattern) == "" {
		return nil, fmt.Errorf("regexp pattern cannot be empty")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("compile regexp pattern %q: %w", pattern, err)
	}

	return &RegexpParser{
		re:          re,
		host:        host,
		source:      source,
		defaultTags: defaultTags,
	}, nil
}

// Parse extracts named capture groups from the matching line.
func (p *RegexpParser) Parse(line string) (logentry.LogEntry, error) {
	trimmed := strings.TrimRight(line, "\r\n")
	if len(trimmed) == 0 {
		return logentry.LogEntry{}, ErrEmptyLine
	}

	match := p.re.FindStringSubmatch(trimmed)
	if match == nil {
		return logentry.LogEntry{}, ErrNoMatch
	}

	entry := logentry.New(p.host, p.source, "INFO", trimmed, p.defaultTags)
	fields := make(map[string]string)
	for k, v := range p.defaultTags {
		fields[k] = v
	}

	names := p.re.SubexpNames()
	for i, name := range names {
		if i == 0 || name == "" {
			continue
		}
		val := match[i]
		lowerName := strings.ToLower(name)
		switch lowerName {
		case "timestamp", "time", "ts":
			if ts, ok := parseTimestamp(val); ok {
				entry.Timestamp = ts
			}
		case "level", "severity", "lvl":
			entry.Level = strings.ToUpper(val)
		case "message", "msg":
			entry.Message = val
		case "host", "hostname":
			if val != "" {
				entry.Host = val
			}
		case "source", "service":
			if val != "" {
				entry.Source = val
			}
		default:
			fields[name] = val
		}
	}

	if len(fields) > 0 {
		entry.Fields = fields
	}

	return entry, nil
}
