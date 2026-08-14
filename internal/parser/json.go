package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

// JSONParser parses structured JSON lines into LogEntries.
type JSONParser struct {
	host        string
	source      string
	defaultTags map[string]string
}

// NewJSONParser creates a new JSONParser.
func NewJSONParser(host, source string, defaultTags map[string]string) *JSONParser {
	return &JSONParser{
		host:        host,
		source:      source,
		defaultTags: defaultTags,
	}
}

// Parse extracts fields from a JSON log line and normalizes standard timestamp/level/message fields.
func (p *JSONParser) Parse(line string) (logentry.LogEntry, error) {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) == 0 {
		return logentry.LogEntry{}, ErrEmptyLine
	}

	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return logentry.LogEntry{}, fmt.Errorf("json unmarshal: %w", err)
	}

	entry := logentry.New(p.host, p.source, "INFO", "", p.defaultTags)

	fields := make(map[string]string)
	for k, v := range p.defaultTags {
		fields[k] = v
	}

	var foundTimestamp bool
	var foundMessage bool
	var foundLevel bool

	for key, val := range raw {
		lowerKey := strings.ToLower(key)
		switch lowerKey {
		case "timestamp", "time", "ts", "@timestamp", "datetime":
			if ts, ok := parseTimestamp(val); ok {
				entry.Timestamp = ts
				foundTimestamp = true
			}
		case "level", "severity", "lvl", "status":
			if strVal, ok := stringify(val); ok && strVal != "" {
				entry.Level = strings.ToUpper(strVal)
				foundLevel = true
			}
		case "message", "msg", "log", "text":
			if strVal, ok := stringify(val); ok {
				entry.Message = strVal
				foundMessage = true
			}
		case "host", "hostname":
			if strVal, ok := stringify(val); ok && strVal != "" {
				entry.Host = strVal
			}
		case "source", "service", "app":
			if strVal, ok := stringify(val); ok && strVal != "" {
				entry.Source = strVal
			}
		default:
			if strVal, ok := stringify(val); ok {
				fields[key] = strVal
			}
		}
	}

	if !foundTimestamp {
		entry.Timestamp = time.Now().UTC()
	}
	if !foundLevel {
		entry.Level = "INFO"
	}
	if !foundMessage {
		// Fallback to complete JSON string if message was not mapped
		entry.Message = trimmed
	}

	if len(fields) > 0 {
		entry.Fields = fields
	}

	return entry, nil
}

func parseTimestamp(val any) (time.Time, bool) {
	switch v := val.(type) {
	case string:
		// Try standard timestamp layouts
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05",
			"2006-01-02 15:04:05.000",
			time.RFC1123,
			time.RFC1123Z,
			time.DateTime,
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, v); err == nil {
				return t.UTC(), true
			}
		}
		// Try string encoded unix timestamp
		if sec, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parseEpoch(sec), true
		}
	case float64:
		return parseEpoch(int64(v)), true
	case int64:
		return parseEpoch(v), true
	case int:
		return parseEpoch(int64(v)), true
	}
	return time.Time{}, false
}

func parseEpoch(epoch int64) time.Time {
	// If epoch is in milliseconds (e.g. > 1e11)
	if epoch > 1e11 && epoch < 1e14 {
		return time.UnixMilli(epoch).UTC()
	}
	// If epoch is in microseconds (e.g. > 1e14)
	if epoch >= 1e14 && epoch < 1e17 {
		return time.UnixMicro(epoch).UTC()
	}
	// If epoch is in nanoseconds (e.g. >= 1e17)
	if epoch >= 1e17 {
		return time.Unix(0, epoch).UTC()
	}
	// Standard seconds
	return time.Unix(epoch, 0).UTC()
}

func stringify(val any) (string, bool) {
	if val == nil {
		return "", false
	}
	switch v := val.(type) {
	case string:
		return v, true
	case bool:
		return strconv.FormatBool(v), true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case int:
		return strconv.Itoa(v), true
	case int64:
		return strconv.FormatInt(v, 10), true
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v), true
		}
		return string(b), true
	}
}
