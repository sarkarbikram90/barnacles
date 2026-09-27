package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const indexFileName = "index.json"

// LoadPartitionIndex reads and parses all block headers from a partition directory's index.json.
func LoadPartitionIndex(dir string) ([]BlockHeader, error) {
	indexPath := filepath.Join(dir, indexFileName)
	data, err := os.ReadFile(indexPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var headers []BlockHeader
	if err := json.Unmarshal(data, &headers); err != nil {
		return nil, fmt.Errorf("unmarshal partition index %q: %w", indexPath, err)
	}

	return headers, nil
}

// AppendBlockToPartition writes a compressed block payload to disk and updates the partition's index.json atomically.
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

	// 2. Load existing index, append new header, and sort chronologically
	existing, _ := LoadPartitionIndex(dir)
	existing = append(existing, header)

	sort.Slice(existing, func(i, j int) bool {
		return existing[i].MinTimestampNano < existing[j].MinTimestampNano
	})

	indexData, err := json.MarshalIndent(existing, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal partition index: %w", err)
	}

	indexPath := filepath.Join(dir, indexFileName)
	tempIndexPath := filepath.Join(dir, fmt.Sprintf(".index.%d.tmp", time.Now().UnixNano()))

	idxFile, err := os.OpenFile(tempIndexPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create temp index %q: %w", tempIndexPath, err)
	}
	if _, err := idxFile.Write(indexData); err != nil {
		_ = idxFile.Close()
		_ = os.Remove(tempIndexPath)
		return fmt.Errorf("write temp index %q: %w", tempIndexPath, err)
	}
	if syncOnWrite {
		_ = idxFile.Sync()
	}
	if err := idxFile.Close(); err != nil {
		_ = os.Remove(tempIndexPath)
		return fmt.Errorf("close temp index %q: %w", tempIndexPath, err)
	}

	if err := os.Rename(tempIndexPath, indexPath); err != nil {
		_ = os.Remove(tempIndexPath)
		return fmt.Errorf("commit partition index %q: %w", indexPath, err)
	}

	return nil
}
