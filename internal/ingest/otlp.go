// Package ingest handles incoming log batch HTTP requests, OTLP ingestion,
// validation, idempotency deduplication, persistence to storage, and live streaming dispatch.
package ingest

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

// OTLPExportLogsServiceRequest represents the top-level OpenTelemetry OTLP JSON payload for /v1/logs.
type OTLPExportLogsServiceRequest struct {
	ResourceLogs []OTLPResourceLogs `json:"resourceLogs"`
}

type OTLPResourceLogs struct {
	Resource  OTLPResource    `json:"resource"`
	ScopeLogs []OTLPScopeLogs `json:"scopeLogs"`
}

type OTLPResource struct {
	Attributes []OTLPKeyValue `json:"attributes"`
}

type OTLPScopeLogs struct {
	Scope      OTLPInstrumentationScope `json:"scope"`
	LogRecords []OTLPLogRecord          `json:"logRecords"`
}

type OTLPInstrumentationScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type OTLPLogRecord struct {
	TimeUnixNano         string         `json:"timeUnixNano"`
	ObservedTimeUnixNano string         `json:"observedTimeUnixNano"`
	SeverityNumber       int            `json:"severityNumber"`
	SeverityText         string         `json:"severityText"`
	Body                 OTLPAnyValue   `json:"body"`
	Attributes           []OTLPKeyValue `json:"attributes"`
	TraceID              string         `json:"traceId"`
	SpanID               string         `json:"spanId"`
}

type OTLPKeyValue struct {
	Key   string       `json:"key"`
	Value OTLPAnyValue `json:"value"`
}

type OTLPAnyValue struct {
	StringValue *string `json:"stringValue,omitempty"`
	BoolValue   *bool   `json:"boolValue,omitempty"`
	IntValue    any     `json:"intValue,omitempty"`
	DoubleValue *float64`json:"doubleValue,omitempty"`
}

func (v *OTLPAnyValue) String() string {
	if v == nil {
		return ""
	}
	if v.StringValue != nil {
		return *v.StringValue
	}
	if v.BoolValue != nil {
		return strconv.FormatBool(*v.BoolValue)
	}
	if v.IntValue != nil {
		return fmt.Sprintf("%v", v.IntValue)
	}
	if v.DoubleValue != nil {
		return strconv.FormatFloat(*v.DoubleValue, 'f', -1, 64)
	}
	return ""
}

// SeverityNumberToLevel maps OpenTelemetry severity numbers (1-24) to canonical log levels.
func SeverityNumberToLevel(num int, text string) string {
	if text != "" {
		return strings.ToUpper(strings.TrimSpace(text))
	}
	switch {
	case num >= 1 && num <= 4:
		return "TRACE"
	case num >= 5 && num <= 8:
		return "DEBUG"
	case num >= 9 && num <= 12:
		return "INFO"
	case num >= 13 && num <= 16:
		return "WARN"
	case num >= 17 && num <= 20:
		return "ERROR"
	case num >= 21 && num <= 24:
		return "FATAL"
	default:
		return "INFO"
	}
}

// ParseOTLPLogsJSON decodes an OTLP JSON stream into normalized Barnacles LogEntries.
func ParseOTLPLogsJSON(r io.Reader) ([]logentry.LogEntry, error) {
	var req OTLPExportLogsServiceRequest
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		return nil, fmt.Errorf("decode OTLP payload: %w", err)
	}

	var entries []logentry.LogEntry

	for _, rLogs := range req.ResourceLogs {
		// Extract host and service from resource attributes
		resourceHost := ""
		resourceService := ""
		resAttrs := make(map[string]string)

		for _, attr := range rLogs.Resource.Attributes {
			val := attr.Value.String()
			resAttrs[attr.Key] = val
			switch attr.Key {
			case "host.name", "host.id", "k8s.node.name":
				if resourceHost == "" {
					resourceHost = val
				}
			case "service.name", "app", "application":
				if resourceService == "" {
					resourceService = val
				}
			}
		}

		if resourceHost == "" {
			resourceHost = "otlp-host"
		}
		if resourceService == "" {
			resourceService = "otlp-service"
		}

		for _, sLogs := range rLogs.ScopeLogs {
			scopeName := sLogs.Scope.Name
			source := resourceService
			if scopeName != "" {
				source = scopeName
			}

			for _, rec := range sLogs.LogRecords {
				ts := time.Now().UTC()
				timeStr := rec.TimeUnixNano
				if timeStr == "" {
					timeStr = rec.ObservedTimeUnixNano
				}
				if timeStr != "" {
					if nano, err := strconv.ParseInt(timeStr, 10, 64); err == nil && nano > 0 {
						ts = time.Unix(0, nano).UTC()
					}
				}

				level := SeverityNumberToLevel(rec.SeverityNumber, rec.SeverityText)
				msg := rec.Body.String()
				if msg == "" {
					msg = "(empty log body)"
				}

				fields := make(map[string]string)
				// Merge resource attributes
				for k, v := range resAttrs {
					fields[k] = v
				}
				// Merge record attributes
				for _, attr := range rec.Attributes {
					fields[attr.Key] = attr.Value.String()
				}
				if rec.TraceID != "" {
					fields["trace_id"] = rec.TraceID
				}
				if rec.SpanID != "" {
					fields["span_id"] = rec.SpanID
				}

				entry := logentry.LogEntry{
					ID:        uuid.NewString(),
					Timestamp: ts,
					Host:      resourceHost,
					Source:    source,
					Level:     level,
					Message:   msg,
					Fields:    fields,
				}
				entries = append(entries, entry)
			}
		}
	}

	return entries, nil
}
