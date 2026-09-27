// Package store provides compressed block persistence, partition metadata indexing,
// and block-pruned log queries for high-performance edge log storage.
package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

const (
	LevelTrace uint16 = 1 << 0
	LevelDebug uint16 = 1 << 1
	LevelInfo  uint16 = 1 << 2
	LevelWarn  uint16 = 1 << 3
	LevelError uint16 = 1 << 4
	LevelFatal uint16 = 1 << 5
)

// LevelToBit returns the bitmask flag for a log level string.
func LevelToBit(level string) uint16 {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "TRACE":
		return LevelTrace
	case "DEBUG":
		return LevelDebug
	case "INFO":
		return LevelInfo
	case "WARN", "WARNING":
		return LevelWarn
	case "ERROR", "ERR":
		return LevelError
	case "FATAL", "PANIC", "CRIT", "CRITICAL":
		return LevelFatal
	default:
		return LevelInfo
	}
}

// BlockHeader contains metadata used for pre-decompression block pruning during queries.
type BlockHeader struct {
	ID                string   `json:"id"`
	FileName          string   `json:"file_name"`
	MinTimestampNano int64    `json:"min_ts_nano"`
	MaxTimestampNano int64    `json:"max_ts_nano"`
	RecordCount       int      `json:"record_count"`
	UncompressedBytes int64    `json:"uncompressed_bytes"`
	CompressedBytes   int64    `json:"compressed_bytes"`
	LevelMask         uint16   `json:"level_mask"`
	Hosts             []string `json:"hosts"`
	Sources           []string `json:"sources"`
}

var (
	zstdEncoderPool sync.Pool
	zstdDecoderPool sync.Pool
)

func getZstdEncoder() (*zstd.Encoder, error) {
	if enc, ok := zstdEncoderPool.Get().(*zstd.Encoder); ok && enc != nil {
		return enc, nil
	}
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
}

func putZstdEncoder(enc *zstd.Encoder) {
	if enc != nil {
		enc.Reset(nil)
		zstdEncoderPool.Put(enc)
	}
}

func getZstdDecoder() (*zstd.Decoder, error) {
	if dec, ok := zstdDecoderPool.Get().(*zstd.Decoder); ok && dec != nil {
		return dec, nil
	}
	return zstd.NewReader(nil)
}

func putZstdDecoder(dec *zstd.Decoder) {
	if dec != nil {
		zstdDecoderPool.Put(dec)
	}
}

// EncodeBlock compresses a slice of LogEntries into a Zstandard block payload
// and calculates its metadata BlockHeader.
func EncodeBlock(entries []logentry.LogEntry) (BlockHeader, []byte, error) {
	if len(entries) == 0 {
		return BlockHeader{}, nil, nil
	}

	var (
		minTsNano int64 = 1<<63 - 1
		maxTsNano int64 = -1
		levelMask uint16
		hostMap   = make(map[string]struct{})
		sourceMap = make(map[string]struct{})
		rawBuf    bytes.Buffer
	)

	for _, entry := range entries {
		ts := entry.Timestamp
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		nano := ts.UnixNano()
		if nano < minTsNano {
			minTsNano = nano
		}
		if nano > maxTsNano {
			maxTsNano = nano
		}

		levelMask |= LevelToBit(entry.Level)
		if entry.Host != "" {
			hostMap[entry.Host] = struct{}{}
		}
		if entry.Source != "" {
			sourceMap[entry.Source] = struct{}{}
		}

		data, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		rawBuf.Write(data)
		rawBuf.WriteByte('\n')
	}

	uncompressed := rawBuf.Bytes()
	enc, err := getZstdEncoder()
	if err != nil {
		return BlockHeader{}, nil, fmt.Errorf("create zstd encoder: %w", err)
	}
	compressed := enc.EncodeAll(uncompressed, make([]byte, 0, len(uncompressed)/2))
	putZstdEncoder(enc)

	var hosts []string
	for h := range hostMap {
		hosts = append(hosts, h)
	}
	var sources []string
	for s := range sourceMap {
		sources = append(sources, s)
	}

	blockID := uuid.NewString()
	fileName := fmt.Sprintf("block_%020d_%s.zst", minTsNano, blockID[:8])

	header := BlockHeader{
		ID:                blockID,
		FileName:          fileName,
		MinTimestampNano: minTsNano,
		MaxTimestampNano: maxTsNano,
		RecordCount:       len(entries),
		UncompressedBytes: int64(len(uncompressed)),
		CompressedBytes:   int64(len(compressed)),
		LevelMask:         levelMask,
		Hosts:             hosts,
		Sources:           sources,
	}

	return header, compressed, nil
}

// DecodeBlock decompresses a Zstandard block payload and parses all contained LogEntries.
func DecodeBlock(compressed []byte) ([]logentry.LogEntry, error) {
	if len(compressed) == 0 {
		return nil, nil
	}

	dec, err := getZstdDecoder()
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	decompressed, err := dec.DecodeAll(compressed, nil)
	putZstdDecoder(dec)
	if err != nil {
		return nil, fmt.Errorf("decompress block: %w", err)
	}

	var entries []logentry.LogEntry
	reader := bytes.NewReader(decompressed)
	decJSON := json.NewDecoder(reader)

	for {
		var entry logentry.LogEntry
		if err := decJSON.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			// Skip corrupted entry or break
			break
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// MatchesQuery returns true if the block potentially contains matching entries,
// or false if the block can be safely pruned before decompression.
func (h *BlockHeader) MatchesQuery(q logentry.Query) bool {
	// 1. Time range pruning
	if !q.StartTime.IsZero() {
		if h.MaxTimestampNano < q.StartTime.UnixNano() {
			return false // Entire block ended before query start
		}
	}
	if !q.EndTime.IsZero() {
		if h.MinTimestampNano > q.EndTime.UnixNano() {
			return false // Entire block started after query end
		}
	}

	// 2. Level bitmask pruning
	if q.Level != "" {
		targetBit := LevelToBit(q.Level)
		if (h.LevelMask & targetBit) == 0 {
			return false // Block contains no events of this severity level
		}
	}

	// 3. Host pruning
	if q.Host != "" {
		matchedHost := false
		for _, h := range h.Hosts {
			if strings.EqualFold(h, q.Host) {
				matchedHost = true
				break
			}
		}
		if !matchedHost {
			return false
		}
	}

	// 4. Source pruning
	if q.Source != "" {
		matchedSrc := false
		for _, s := range h.Sources {
			if strings.EqualFold(s, q.Source) {
				matchedSrc = true
				break
			}
		}
		if !matchedSrc {
			return false
		}
	}

	return true
}
