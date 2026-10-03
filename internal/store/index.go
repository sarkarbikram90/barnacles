package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const (
	indexJSONLFileName = "index.jsonl"
	indexJSONFileName  = "index.json" // legacy fallback
)

// LoadPartitionIndex reads and parses all block headers from a partition directory.
// It supports high-performance append-only index.jsonl as well as legacy index.json.
func LoadPartitionIndex(dir string) ([]BlockHeader, error) {
	var headers []BlockHeader
	seenFiles := make(map[string]struct{})

	// 1. Check legacy index.json (for backwards compatibility with existing clusters)
	legacyPath := filepath.Join(dir, indexJSONFileName)
	if legacyData, err := os.ReadFile(legacyPath); err == nil {
		var legacyHeaders []BlockHeader
		if err := json.Unmarshal(legacyData, &legacyHeaders); err == nil {
			for _, h := range legacyHeaders {
				if _, ok := seenFiles[h.FileName]; !ok {
					seenFiles[h.FileName] = struct{}{}
					headers = append(headers, h)
				}
			}
		}
	}

	// 2. Read append-only index.jsonl
	jsonlPath := filepath.Join(dir, indexJSONLFileName)
	f, err := os.Open(jsonlPath)
	if err == nil {
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanBuf := make([]byte, 64*1024)
		scanner.Buffer(scanBuf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var h BlockHeader
			if err := json.Unmarshal(line, &h); err == nil {
				if _, ok := seenFiles[h.FileName]; !ok {
					seenFiles[h.FileName] = struct{}{}
					headers = append(headers, h)
				}
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read append index %q: %w", jsonlPath, err)
	}

	if len(headers) == 0 {
		return nil, nil
	}

	sort.Slice(headers, func(i, j int) bool {
		return headers[i].MinTimestampNano < headers[j].MinTimestampNano
	})

	return headers, nil
}

// AppendBlockToPartition writes a compressed block payload to disk and appends
// its header to the partition's append-only index.jsonl with O(1) performance.
func AppendBlockToPartition(dir string, header BlockHeader, payload []byte, syncOnWrite bool) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create partition dir %q: %w", dir, err)
	}

	blockPath := filepath.Join(dir, header.FileName)
	tempBlockPath := filepath.Join(dir, "."+header.FileName+".tmp")

	// 1. Write compressed block payload atomically
	f, err := os.OpenFile(tempBlockPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create temp block %q: %w", tempBlockPath, err)
	}
	if _, err := f.Write(payload); err != nil {
		_ = f.Close()
		_ = os.Remove(tempBlockPath)
		return fmt.Errorf("write block payload %q: %w", tempBlockPath, err)
	}
	if syncOnWrite {
		_ = f.Sync()
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tempBlockPath)
		return fmt.Errorf("close temp block %q: %w", tempBlockPath, err)
	}

	if err := os.Rename(tempBlockPath, blockPath); err != nil {
		_ = os.Remove(tempBlockPath)
		return fmt.Errorf("commit block file %q: %w", blockPath, err)
	}

	// 2. Append header to append-only index.jsonl (O(1) write without rewrite amplification)
	line, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("marshal partition index header: %w", err)
	}
	line = append(line, '\n')

	indexPath := filepath.Join(dir, indexJSONLFileName)
	idxFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open append-only index %q: %w", indexPath, err)
	}
	if _, err := idxFile.Write(line); err != nil {
		_ = idxFile.Close()
		return fmt.Errorf("append to index %q: %w", indexPath, err)
	}
	if syncOnWrite {
		_ = idxFile.Sync()
	}
	if err := idxFile.Close(); err != nil {
		return fmt.Errorf("close append index %q: %w", indexPath, err)
	}

	return nil
}

// RewritePartitionIndex atomically replaces a partition's index.jsonl with a consolidated set of headers.
// Used by compaction to update partition metadata after consolidating small blocks into larger blocks.
func RewritePartitionIndex(dir string, headers []BlockHeader, syncOnWrite bool) error {
	indexPath := filepath.Join(dir, indexJSONLFileName)
	tempPath := filepath.Join(dir, "."+indexJSONLFileName+".tmp")

	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create temp index %q: %w", tempPath, err)
	}

	for _, h := range headers {
		data, err := json.Marshal(h)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tempPath)
			return fmt.Errorf("marshal header %q: %w", h.ID, err)
		}
		data = append(data, '\n')
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			_ = os.Remove(tempPath)
			return fmt.Errorf("write header to temp index %q: %w", tempPath, err)
		}
	}

	if syncOnWrite {
		_ = f.Sync()
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close temp index %q: %w", tempPath, err)
	}

	if err := os.Rename(tempPath, indexPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("commit rewritten index %q: %w", indexPath, err)
	}

	// Clean up legacy index.json if present
	_ = os.Remove(filepath.Join(dir, indexJSONFileName))

	return nil
}
