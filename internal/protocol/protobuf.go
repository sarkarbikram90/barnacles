// Package protocol implements high-performance binary wire serialization for
// Barnacles log events, ingest requests, and responses compliant with the
// standard Protocol Buffers v3 specification.
package protocol

import (
	"errors"
	"strings"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	// ContentTypeJSON is the MIME type for JSON payloads.
	ContentTypeJSON = "application/json"
	// ContentTypeProtobuf is the primary MIME type for binary Protocol Buffers payloads.
	ContentTypeProtobuf = "application/x-protobuf"
	// ContentTypeOctetStream is an alias accepted for binary protocol payloads.
	ContentTypeOctetStream = "application/octet-stream"
)

// IsProtobufContentType returns true if the content-type header indicates protobuf binary wire format.
func IsProtobufContentType(contentType string) bool {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	return strings.HasPrefix(ct, ContentTypeProtobuf) || strings.HasPrefix(ct, ContentTypeOctetStream)
}

// MarshalLogEntry encodes a single LogEntry into standard proto3 wire binary format.
func MarshalLogEntry(e *logentry.LogEntry) []byte {
	sizeEst := 64 + len(e.ID) + len(e.Host) + len(e.Source) + len(e.Level) + len(e.Message)
	for k, v := range e.Fields {
		sizeEst += 16 + len(k) + len(v)
	}
	return AppendLogEntry(make([]byte, 0, sizeEst), e)
}

// AppendLogEntry appends the proto3 binary wire encoding of e to dst and returns the updated slice.
func AppendLogEntry(dst []byte, e *logentry.LogEntry) []byte {
	b := dst
	if e.ID != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, e.ID)
	}
	if !e.Timestamp.IsZero() {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(e.Timestamp.UnixNano()))
	}
	if e.Host != "" {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendString(b, e.Host)
	}
	if e.Source != "" {
		b = protowire.AppendTag(b, 4, protowire.BytesType)
		b = protowire.AppendString(b, e.Source)
	}
	if e.Level != "" {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendString(b, e.Level)
	}
	if e.Message != "" {
		b = protowire.AppendTag(b, 6, protowire.BytesType)
		b = protowire.AppendString(b, e.Message)
	}
	if len(e.Fields) > 0 {
		for k, v := range e.Fields {
			b = protowire.AppendTag(b, 7, protowire.BytesType)
			var entryBytes []byte
			if k != "" {
				entryBytes = protowire.AppendTag(entryBytes, 1, protowire.BytesType)
				entryBytes = protowire.AppendString(entryBytes, k)
			}
			if v != "" {
				entryBytes = protowire.AppendTag(entryBytes, 2, protowire.BytesType)
				entryBytes = protowire.AppendString(entryBytes, v)
			}
			b = protowire.AppendBytes(b, entryBytes)
		}
	}
	return b
}

// UnmarshalLogEntry decodes a single LogEntry from proto3 wire binary format.
func UnmarshalLogEntry(b []byte, e *logentry.LogEntry) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			e.ID = v
			b = b[n:]
		case 2:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			if v != 0 {
				e.Timestamp = time.Unix(0, int64(v)).UTC()
			}
			b = b[n:]
		case 3:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			e.Host = v
			b = b[n:]
		case 4:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			e.Source = v
			b = b[n:]
		case 5:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			e.Level = v
			b = b[n:]
		case 6:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			e.Message = v
			b = b[n:]
		case 7:
			entryBytes, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			k, v, err := unmarshalMapEntry(entryBytes)
			if err != nil {
				return err
			}
			if e.Fields == nil {
				e.Fields = make(map[string]string)
			}
			e.Fields[k] = v
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

func unmarshalMapEntry(b []byte) (string, string, error) {
	var k, v string
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			val, n := protowire.ConsumeString(b)
			if n < 0 {
				return "", "", protowire.ParseError(n)
			}
			k = val
			b = b[n:]
		case 2:
			val, n := protowire.ConsumeString(b)
			if n < 0 {
				return "", "", protowire.ParseError(n)
			}
			v = val
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return "", "", protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return k, v, nil
}

// MarshalIngestRequest encodes an IngestRequest into standard proto3 wire binary format.
func MarshalIngestRequest(req *logentry.IngestRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.New("cannot marshal nil IngestRequest")
	}
	estSize := len(req.AgentID) + 8 + len(req.Events)*256
	b := make([]byte, 0, estSize)
	if req.AgentID != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, req.AgentID)
	}
	for i := range req.Events {
		eventBytes := MarshalLogEntry(&req.Events[i])
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendBytes(b, eventBytes)
	}
	return b, nil
}

// UnmarshalIngestRequest decodes an IngestRequest from proto3 wire binary format.
func UnmarshalIngestRequest(b []byte, req *logentry.IngestRequest) error {
	if req == nil {
		return errors.New("cannot unmarshal into nil IngestRequest")
	}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			req.AgentID = v
			b = b[n:]
		case 2:
			eventBytes, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			var entry logentry.LogEntry
			if err := UnmarshalLogEntry(eventBytes, &entry); err != nil {
				return err
			}
			req.Events = append(req.Events, entry)
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

// MarshalIngestResponse encodes an IngestResponse into standard proto3 wire binary format.
func MarshalIngestResponse(resp *logentry.IngestResponse) ([]byte, error) {
	if resp == nil {
		return nil, errors.New("cannot marshal nil IngestResponse")
	}
	var b []byte
	if resp.Status != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, resp.Status)
	}
	if resp.Accepted != 0 {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(resp.Accepted))
	}
	if resp.Duplicates != 0 {
		b = protowire.AppendTag(b, 3, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(resp.Duplicates))
	}
	for _, errMsg := range resp.Errors {
		b = protowire.AppendTag(b, 4, protowire.BytesType)
		b = protowire.AppendString(b, errMsg)
	}
	return b, nil
}

// UnmarshalIngestResponse decodes an IngestResponse from proto3 wire binary format.
func UnmarshalIngestResponse(b []byte, resp *logentry.IngestResponse) error {
	if resp == nil {
		return errors.New("cannot unmarshal into nil IngestResponse")
	}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			resp.Status = v
			b = b[n:]
		case 2:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			resp.Accepted = int(v)
			b = b[n:]
		case 3:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			resp.Duplicates = int(v)
			b = b[n:]
		case 4:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			resp.Errors = append(resp.Errors, v)
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}
