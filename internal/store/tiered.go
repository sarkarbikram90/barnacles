// Package store defines the storage interface and filesystem-backed log persistence
// supporting time-segmented indexing, querying, retention pruning, compaction, and tiered archiving.
package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ObjectStore defines the abstraction for remote or tiered object storage (S3, GCS, MinIO, or LocalArchive).
type ObjectStore interface {
	// PutObject stores a binary object at the specified key.
	PutObject(ctx context.Context, key string, data []byte) error

	// GetObject retrieves a binary object by key.
	GetObject(ctx context.Context, key string) ([]byte, error)

	// ListKeys lists all object keys matching a prefix.
	ListKeys(ctx context.Context, prefix string) ([]string, error)

	// DeletePrefix deletes all objects under a prefix.
	DeletePrefix(ctx context.Context, prefix string) error
}

// LocalArchiveStore implements ObjectStore backed by a local filesystem directory,
// enabling local simulation and unit testing of S3/GCS object storage bucket semantics.
type LocalArchiveStore struct {
	mu      sync.RWMutex
	rootDir string
}

// NewLocalArchiveStore initializes a LocalArchiveStore under rootDir.
func NewLocalArchiveStore(rootDir string) (*LocalArchiveStore, error) {
	if strings.TrimSpace(rootDir) == "" {
		return nil, fmt.Errorf("archive root directory cannot be empty")
	}
	if err := os.MkdirAll(rootDir, 0o750); err != nil {
		return nil, fmt.Errorf("create archive directory %q: %w", rootDir, err)
	}
	return &LocalArchiveStore{rootDir: rootDir}, nil
}

// PutObject writes data to a path structured as rootDir/key.
func (a *LocalArchiveStore) PutObject(ctx context.Context, key string, data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	targetPath := filepath.Join(a.rootDir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o750); err != nil {
		return fmt.Errorf("create object parent directory: %w", err)
	}

	tempPath := targetPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		return fmt.Errorf("write object %q: %w", key, err)
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("commit object %q: %w", key, err)
	}
	return nil
}

// GetObject reads object data by key.
func (a *LocalArchiveStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	targetPath := filepath.Join(a.rootDir, filepath.FromSlash(key))
	data, err := os.ReadFile(targetPath)
	if err != nil {
		return nil, fmt.Errorf("read object %q: %w", key, err)
	}
	return data, nil
}

// ListKeys returns all keys under prefix.
func (a *LocalArchiveStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var keys []string
	cleanPrefix := filepath.FromSlash(prefix)
	searchRoot := filepath.Join(a.rootDir, cleanPrefix)

	_ = filepath.Walk(searchRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(a.rootDir, path)
		if relErr == nil {
			keys = append(keys, filepath.ToSlash(rel))
		}
		return nil
	})
	return keys, nil
}

// DeletePrefix removes all files under prefix.
func (a *LocalArchiveStore) DeletePrefix(ctx context.Context, prefix string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	targetPath := filepath.Join(a.rootDir, filepath.FromSlash(prefix))
	return os.RemoveAll(targetPath)
}

// TieredArchiver manages offloading sealed partitions to an ObjectStore cold tier.
type TieredArchiver struct {
	store       *FileStore
	objectStore ObjectStore
	coldAge     time.Duration
}

// NewTieredArchiver creates an archiver that offloads partitions older than coldAge.
func NewTieredArchiver(store *FileStore, objectStore ObjectStore, coldAge time.Duration) *TieredArchiver {
	if coldAge <= 0 {
		coldAge = 24 * time.Hour
	}
	return &TieredArchiver{
		store:       store,
		objectStore: objectStore,
		coldAge:     coldAge,
	}
}

// ArchivePartition uploads a partition's index and data blocks to object storage.
// Returns the count of files uploaded and total bytes transferred.
func (ta *TieredArchiver) ArchivePartition(ctx context.Context, partitionDir string) (int, int64, error) {
	relPath, err := filepath.Rel(ta.store.rootDir, partitionDir)
	if err != nil {
		return 0, 0, fmt.Errorf("resolve relative partition path: %w", err)
	}
	prefix := filepath.ToSlash(relPath)

	entries, err := os.ReadDir(partitionDir)
	if err != nil {
		return 0, 0, fmt.Errorf("read partition dir %q: %w", partitionDir, err)
	}

	uploaded := 0
	var totalBytes int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Only archive data blocks and index files
		if !strings.HasSuffix(name, ".zst") && name != indexJSONLFileName && name != indexJSONFileName {
			continue
		}

		filePath := filepath.Join(partitionDir, name)
		data, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return uploaded, totalBytes, fmt.Errorf("read file %q: %w", filePath, readErr)
		}

		objectKey := fmt.Sprintf("partitions/%s/%s", prefix, name)
		if putErr := ta.objectStore.PutObject(ctx, objectKey, data); putErr != nil {
			return uploaded, totalBytes, fmt.Errorf("upload %q to object store: %w", objectKey, putErr)
		}

		uploaded++
		totalBytes += int64(len(data))
	}

	return uploaded, totalBytes, nil
}

// RestorePartition downloads an archived partition from object storage to the local FileStore.
func (ta *TieredArchiver) RestorePartition(ctx context.Context, partitionKey string) (int, int64, error) {
	prefix := fmt.Sprintf("partitions/%s/", filepath.ToSlash(partitionKey))
	keys, err := ta.objectStore.ListKeys(ctx, prefix)
	if err != nil {
		return 0, 0, fmt.Errorf("list partition objects for %q: %w", partitionKey, err)
	}

	if len(keys) == 0 {
		return 0, 0, fmt.Errorf("no archived objects found for partition %q", partitionKey)
	}

	targetDir := filepath.Join(ta.store.rootDir, filepath.FromSlash(partitionKey))
	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return 0, 0, fmt.Errorf("create restore partition dir %q: %w", targetDir, err)
	}

	restored := 0
	var totalBytes int64

	for _, key := range keys {
		data, getErr := ta.objectStore.GetObject(ctx, key)
		if getErr != nil {
			return restored, totalBytes, fmt.Errorf("get object %q: %w", key, getErr)
		}

		fileName := filepath.Base(filepath.FromSlash(key))
		destPath := filepath.Join(targetDir, fileName)
		if writeErr := os.WriteFile(destPath, data, 0o600); writeErr != nil {
			return restored, totalBytes, fmt.Errorf("write restored file %q: %w", destPath, writeErr)
		}

		restored++
		totalBytes += int64(len(data))
	}

	// Reload headers in FileStore cache
	headers, _ := LoadPartitionIndex(targetDir)
	if len(headers) > 0 {
		ta.store.mu.Lock()
		ta.store.partitionCache[targetDir] = headers
		ta.store.partitionSummaries[targetDir] = ta.store.buildPartitionSummaryLocked(targetDir, headers)
		ta.store.totalBytes += totalBytes
		ta.store.mu.Unlock()
	}

	return restored, totalBytes, nil
}
