package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
)

var (
	// ErrStoreClosed is returned when operations are executed on a closed store.
	ErrStoreClosed = errors.New("log store is closed")
)

// FileStore persists log entries into time-segmented Zstandard compressed blocks
// with partition indexing and block-pruned queries.
type FileStore struct {
	rootDir     string
	syncOnWrite bool

	mu         sync.RWMutex
	closed     bool
	totalBytes int64
	knownHosts map[string]struct{}
	knownSrcs  map[string]struct{}
}

// Compile-time interface check.
var _ LogStore = (*FileStore)(nil)

// Config defines filesystem storage options.
type Config struct {
	Directory   string
	SyncOnWrite bool
}

// NewFileStore initializes the filesystem storage directory and partition index.
func NewFileStore(cfg Config) (*FileStore, error) {
	if strings.TrimSpace(cfg.Directory) == "" {
		return nil, errors.New("storage directory cannot be empty")
	}

	if err := os.MkdirAll(cfg.Directory, 0o750); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	fsStore := &FileStore{
		rootDir:     cfg.Directory,
		syncOnWrite: cfg.SyncOnWrite,
		knownHosts:  make(map[string]struct{}),
		knownSrcs:   make(map[string]struct{}),
	}

	if err := fsStore.scanInitial(); err != nil {
		return nil, fmt.Errorf("initial storage scan: %w", err)
	}

	return fsStore, nil
}

func (s *FileStore) scanInitial() error {
	var total int64

	err := filepath.WalkDir(s.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err == nil {
			total += info.Size()
		}

		// Load headers from index.json to populate known hosts and sources
		if d.Name() == indexFileName {
			headers, err := LoadPartitionIndex(filepath.Dir(path))
			if err == nil {
				for _, h := range headers {
					for _, host := range h.Hosts {
						if host != "" {
							s.knownHosts[host] = struct{}{}
						}
					}
					for _, src := range h.Sources {
						if src != "" {
							s.knownSrcs[src] = struct{}{}
						}
					}
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	s.totalBytes = total
	return nil
}

// Append compresses a batch of log entries into Zstandard blocks partitioned by hour,
// writes them to disk, and updates each partition's index.json.
func (s *FileStore) Append(ctx context.Context, entries []logentry.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	// Group entries by partition directory: YYYY/MM/DD/HH
	grouped := make(map[string][]logentry.LogEntry)

	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		ts := entry.Timestamp
		if ts.IsZero() {
			ts = time.Now().UTC()
		}

		partitionDir := filepath.Join(
			s.rootDir,
			ts.Format("2006"),
			ts.Format("01"),
			ts.Format("02"),
			ts.Format("15"),
		)

		grouped[partitionDir] = append(grouped[partitionDir], entry)

		if entry.Host != "" {
			s.knownHosts[entry.Host] = struct{}{}
		}
		if entry.Source != "" {
			s.knownSrcs[entry.Source] = struct{}{}
		}
	}

	for dir, groupEntries := range grouped {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		header, compressedPayload, err := EncodeBlock(groupEntries)
		if err != nil {
			return fmt.Errorf("encode compressed block for %q: %w", dir, err)
		}

		if err := AppendBlockToPartition(dir, header, compressedPayload, s.syncOnWrite); err != nil {
			return fmt.Errorf("append block to partition %q: %w", dir, err)
		}

		s.totalBytes += header.CompressedBytes
	}

	return nil
}

// Query searches stored log files using block-pruning across partition indices.
// Blocks are skipped before decompression if their time bounds, severity bitmask,
// host, or source filters do not intersect with the query.
func (s *FileStore) Query(ctx context.Context, q logentry.Query) ([]logentry.LogEntry, error) {
	q.Normalize()

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	s.mu.RUnlock()

	var matched []logentry.LogEntry

	// Step 1: Discover candidate partition directories (sorted newest to oldest)
	dirs, err := s.collectCandidateDirs(q)
	if err != nil {
		return nil, fmt.Errorf("collect candidate partition directories: %w", err)
	}

	// Step 2: Search blocks within candidate partitions
	for _, dir := range dirs {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if len(matched) >= q.Limit {
			break
		}

		// Read partition index for pre-decompression block pruning
		headers, err := LoadPartitionIndex(dir)
		if err == nil && len(headers) > 0 {
			// Iterate blocks in reverse order (newest blocks first)
			for i := len(headers) - 1; i >= 0; i-- {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if len(matched) >= q.Limit {
					break
				}

				header := headers[i]

				// High-Performance Pruning: Skip block if metadata doesn't intersect
				if !header.MatchesQuery(q) {
					continue
				}

				// Block matched metadata: decompress and filter
				blockFile := filepath.Join(dir, header.FileName)
				compressed, err := os.ReadFile(blockFile)
				if err != nil {
					continue
				}

				entries, err := DecodeBlock(compressed)
				if err != nil {
					continue
				}

				// Reverse block entries so latest are appended first
				for j := len(entries) - 1; j >= 0; j-- {
					if len(matched) >= q.Limit {
						break
					}
					if entries[j].Matches(q) {
						matched = append(matched, entries[j])
					}
				}
			}
		}

		// Step 3: Backward compatibility with legacy v0.1 uncompressed .log files
		_ = s.queryLegacyFilesInDir(ctx, dir, q, &matched)
	}

	return matched, nil
}

func (s *FileStore) collectCandidateDirs(q logentry.Query) ([]string, error) {
	var allDirs []string

	err := filepath.WalkDir(s.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Check if this is an hourly leaf directory: root/YYYY/MM/DD/HH
			rel, relErr := filepath.Rel(s.rootDir, path)
			if relErr == nil && len(strings.Split(filepath.ToSlash(rel), "/")) == 4 {
				allDirs = append(allDirs, path)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Sort lexicographically reverse (newest directories first)
	sort.Slice(allDirs, func(i, j int) bool {
		return allDirs[i] > allDirs[j]
	})

	// Time Partition Pruning: filter directories that are outside [StartTime, EndTime]
	if q.StartTime.IsZero() && q.EndTime.IsZero() {
		return allDirs, nil
	}

	var candidateDirs []string
	for _, dir := range allDirs {
		rel, err := filepath.Rel(s.rootDir, dir)
		if err != nil {
			candidateDirs = append(candidateDirs, dir)
			continue
		}

		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) == 4 {
			// YYYY/MM/DD/HH
			layout := "2006/01/02/15"
			dirTime, parseErr := time.Parse(layout, strings.Join(parts, "/"))
			if parseErr == nil {
				dirEnd := dirTime.Add(1 * time.Hour)
				if !q.StartTime.IsZero() && dirEnd.Before(q.StartTime) {
					continue // Directory is strictly before query start time
				}
				if !q.EndTime.IsZero() && dirTime.After(q.EndTime) {
					continue // Directory is strictly after query end time
				}
			}
		}

		candidateDirs = append(candidateDirs, dir)
	}

	return candidateDirs, nil
}

func (s *FileStore) queryLegacyFilesInDir(ctx context.Context, dir string, q logentry.Query, matched *[]logentry.LogEntry) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		if len(*matched) >= q.Limit {
			break
		}

		filePath := filepath.Join(dir, entry.Name())
		f, err := os.Open(filePath)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(f)
		scanBuf := make([]byte, 64*1024)
		scanner.Buffer(scanBuf, 1024*1024)

		var legacyEntries []logentry.LogEntry
		for scanner.Scan() {
			if ctx.Err() != nil {
				_ = f.Close()
				return ctx.Err()
			}
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var le logentry.LogEntry
			if err := json.Unmarshal(line, &le); err == nil && le.Matches(q) {
				legacyEntries = append(legacyEntries, le)
			}
		}
		_ = f.Close()

		for j := len(legacyEntries) - 1; j >= 0; j-- {
			if len(*matched) >= q.Limit {
				break
			}
			*matched = append(*matched, legacyEntries[j])
		}
	}
	return nil
}

// Sources returns distinct known sources.
func (s *FileStore) Sources(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]string, 0, len(s.knownSrcs))
	for src := range s.knownSrcs {
		result = append(result, src)
	}
	sort.Strings(result)
	return result, nil
}

// Hosts returns distinct known hosts.
func (s *FileStore) Hosts(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]string, 0, len(s.knownHosts))
	for host := range s.knownHosts {
		result = append(result, host)
	}
	sort.Strings(result)
	return result, nil
}

// DiskUsage returns total disk usage in bytes.
func (s *FileStore) DiskUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalBytes
}

// Prune removes old blocks and legacy files exceeding maxAge or maxSizeBytes.
func (s *FileStore) Prune(ctx context.Context, maxAge time.Duration, maxSizeBytes int64) (int, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, 0, ErrStoreClosed
	}

	var allFiles []string
	_ = filepath.WalkDir(s.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && (strings.HasSuffix(d.Name(), ".zst") || strings.HasSuffix(d.Name(), ".log")) {
			allFiles = append(allFiles, path)
		}
		return nil
	})

	sort.Strings(allFiles) // Oldest first

	var deleted int
	var freed int64
	now := time.Now()

	for _, path := range allFiles {
		if ctx.Err() != nil {
			return deleted, freed, ctx.Err()
		}

		info, err := os.Stat(path)
		if err != nil {
			continue
		}

		shouldDelete := false
		if maxAge > 0 && now.Sub(info.ModTime()) > maxAge {
			shouldDelete = true
		} else if maxSizeBytes > 0 && (s.totalBytes-freed) > maxSizeBytes {
			shouldDelete = true
		}

		if shouldDelete {
			fileSize := info.Size()
			if err := os.Remove(path); err == nil {
				deleted++
				freed += fileSize
				s.totalBytes -= fileSize
			}
		}
	}

	return deleted, freed, nil
}

// Close closes the file store.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
