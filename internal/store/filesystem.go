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

// PartitionSummary contains aggregated multi-dimensional metadata for an hourly partition,
// enabling O(1) partition pruning across time bounds, severity bitmask, host, and source
// before reading or decompressing any block headers.
type PartitionSummary struct {
	Dir        string
	MinTime    time.Time
	MaxTime    time.Time
	LevelMask  uint16
	Hosts      map[string]struct{}
	Sources    map[string]struct{}
	BlockCount int
	TotalBytes int64
}

// MatchesQuery returns false if the partition cannot possibly contain matching log entries.
func (ps *PartitionSummary) MatchesQuery(q logentry.Query) bool {
	if ps == nil {
		return true
	}
	if !q.StartTime.IsZero() && !ps.MaxTime.IsZero() && ps.MaxTime.Before(q.StartTime) {
		return false
	}
	if !q.EndTime.IsZero() && !ps.MinTime.IsZero() && ps.MinTime.After(q.EndTime) {
		return false
	}
	if q.Level != "" {
		bit := LevelToBit(q.Level)
		if bit != 0 && (ps.LevelMask&bit) == 0 {
			return false
		}
	}
	if q.Host != "" {
		if _, ok := ps.Hosts[q.Host]; !ok {
			return false
		}
	}
	if q.Source != "" {
		if _, ok := ps.Sources[q.Source]; !ok {
			return false
		}
	}
	return true
}

// FileStore persists log entries into time-segmented Zstandard compressed blocks
// with partition indexing, partition-level O(1) pruning, compaction, and block-pruned queries.
type FileStore struct {
	rootDir     string
	syncOnWrite bool

	mu                 sync.RWMutex
	closed             bool
	totalBytes         int64
	knownHosts         map[string]struct{}
	knownSrcs          map[string]struct{}
	partitionCache     map[string][]BlockHeader
	partitionSummaries map[string]*PartitionSummary
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
		rootDir:            cfg.Directory,
		syncOnWrite:        cfg.SyncOnWrite,
		knownHosts:         make(map[string]struct{}),
		knownSrcs:          make(map[string]struct{}),
		partitionCache:     make(map[string][]BlockHeader),
		partitionSummaries: make(map[string]*PartitionSummary),
	}

	if err := fsStore.scanInitial(); err != nil {
		return nil, fmt.Errorf("initial storage scan: %w", err)
	}

	return fsStore, nil
}

func (s *FileStore) scanInitial() error {
	var total int64
	visitedPartitions := make(map[string]struct{})

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

		// Load headers from index to populate known hosts, sources, partition cache, and summaries
		if d.Name() == indexJSONLFileName || d.Name() == indexJSONFileName {
			dir := filepath.Dir(path)
			if _, visited := visitedPartitions[dir]; !visited {
				visitedPartitions[dir] = struct{}{}
				headers, err := LoadPartitionIndex(dir)
				if err == nil {
					s.partitionCache[dir] = headers
					s.partitionSummaries[dir] = s.buildPartitionSummaryLocked(dir, headers)
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

		s.partitionCache[dir] = append(s.partitionCache[dir], header)
		s.updatePartitionSummaryLocked(dir, header)
		s.totalBytes += header.CompressedBytes
	}

	return nil
}

func (s *FileStore) buildPartitionSummaryLocked(dir string, headers []BlockHeader) *PartitionSummary {
	if len(headers) == 0 {
		return nil
	}
	summary := &PartitionSummary{
		Dir:        dir,
		Hosts:      make(map[string]struct{}),
		Sources:    make(map[string]struct{}),
		BlockCount: len(headers),
	}
	for i, h := range headers {
		minT := time.Unix(0, h.MinTimestampNano).UTC()
		maxT := time.Unix(0, h.MaxTimestampNano).UTC()
		if i == 0 || minT.Before(summary.MinTime) {
			summary.MinTime = minT
		}
		if i == 0 || maxT.After(summary.MaxTime) {
			summary.MaxTime = maxT
		}
		summary.LevelMask |= h.LevelMask
		summary.TotalBytes += h.CompressedBytes
		for _, host := range h.Hosts {
			if host != "" {
				summary.Hosts[host] = struct{}{}
			}
		}
		for _, src := range h.Sources {
			if src != "" {
				summary.Sources[src] = struct{}{}
			}
		}
	}
	return summary
}

func (s *FileStore) updatePartitionSummaryLocked(dir string, h BlockHeader) {
	summary, ok := s.partitionSummaries[dir]
	if !ok || summary == nil {
		summary = &PartitionSummary{
			Dir:     dir,
			Hosts:   make(map[string]struct{}),
			Sources: make(map[string]struct{}),
		}
		s.partitionSummaries[dir] = summary
	}
	minT := time.Unix(0, h.MinTimestampNano).UTC()
	maxT := time.Unix(0, h.MaxTimestampNano).UTC()
	if summary.MinTime.IsZero() || minT.Before(summary.MinTime) {
		summary.MinTime = minT
	}
	if summary.MaxTime.IsZero() || maxT.After(summary.MaxTime) {
		summary.MaxTime = maxT
	}
	summary.LevelMask |= h.LevelMask
	summary.BlockCount++
	summary.TotalBytes += h.CompressedBytes
	for _, host := range h.Hosts {
		if host != "" {
			summary.Hosts[host] = struct{}{}
		}
	}
	for _, src := range h.Sources {
		if src != "" {
			summary.Sources[src] = struct{}{}
		}
	}
}

func (s *FileStore) getPartitionHeaders(dir string) []BlockHeader {
	s.mu.RLock()
	cached, ok := s.partitionCache[dir]
	s.mu.RUnlock()
	if ok {
		return cached
	}

	headers, err := LoadPartitionIndex(dir)
	if err != nil || len(headers) == 0 {
		return nil
	}

	s.mu.Lock()
	s.partitionCache[dir] = headers
	s.partitionSummaries[dir] = s.buildPartitionSummaryLocked(dir, headers)
	s.mu.Unlock()
	return headers
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

		// Partition Pruning: skip entire partition in O(1) if summary does not match
		s.mu.RLock()
		summary, hasSummary := s.partitionSummaries[dir]
		s.mu.RUnlock()
		if hasSummary && summary != nil && !summary.MatchesQuery(q) {
			continue // Skip entire hourly partition in O(1)!
		}

		// Read partition index for pre-decompression block pruning (from memory cache or disk)
		headers := s.getPartitionHeaders(dir)
		if len(headers) > 0 {
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
	// Fast path: when both time bounds are provided, generate partition paths
	// mathematically rather than walking the entire storage tree.
	// This changes cost from O(total_history) to O(query_window_hours).
	if !q.StartTime.IsZero() && !q.EndTime.IsZero() {
		return s.generateCandidateDirs(q.StartTime, q.EndTime), nil
	}

	// If only one bound is provided, use it to cap the walk
	if !q.StartTime.IsZero() || !q.EndTime.IsZero() {
		return s.walkAndFilterDirs(q)
	}

	// Unbounded query: walk the entire tree (sorted newest first)
	return s.walkAllDirs()
}

// generateCandidateDirs produces hourly partition paths by iterating from
// startTime to endTime in 1-hour increments and checking existence via os.Stat.
// Returns directories sorted newest-first for query result ordering.
func (s *FileStore) generateCandidateDirs(startTime, endTime time.Time) []string {
	// Truncate start to hour boundary
	start := startTime.UTC().Truncate(time.Hour)
	end := endTime.UTC()

	var dirs []string
	for t := start; !t.After(end); t = t.Add(time.Hour) {
		dir := filepath.Join(
			s.rootDir,
			t.Format("2006"),
			t.Format("01"),
			t.Format("02"),
			t.Format("15"),
		)
		if _, err := os.Stat(dir); err == nil {
			dirs = append(dirs, dir)
		}
	}

	// Sort newest first for query result ordering
	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i] > dirs[j]
	})
	return dirs
}

func (s *FileStore) walkAllDirs() ([]string, error) {
	var allDirs []string

	err := filepath.WalkDir(s.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
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

	// Sort newest first
	sort.Slice(allDirs, func(i, j int) bool {
		return allDirs[i] > allDirs[j]
	})
	return allDirs, nil
}

func (s *FileStore) walkAndFilterDirs(q logentry.Query) ([]string, error) {
	allDirs, err := s.walkAllDirs()
	if err != nil {
		return nil, err
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
			layout := "2006/01/02/15"
			dirTime, parseErr := time.Parse(layout, strings.Join(parts, "/"))
			if parseErr == nil {
				dirEnd := dirTime.Add(1 * time.Hour)
				if !q.StartTime.IsZero() && dirEnd.Before(q.StartTime) {
					continue
				}
				if !q.EndTime.IsZero() && dirTime.After(q.EndTime) {
					continue
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
// After pruning blocks, it also removes orphaned index files and empty partition directories.
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

	if deleted > 0 {
		s.partitionCache = make(map[string][]BlockHeader)
		// Clean up orphaned index files and empty partition directories
		s.cleanEmptyPartitionsLocked()
	}

	return deleted, freed, nil
}

// cleanEmptyPartitionsLocked removes index files and empty directories for partitions
// that no longer contain any data blocks (.zst or .log files).
// Must be called with s.mu held.
func (s *FileStore) cleanEmptyPartitionsLocked() {
	var emptyDirs []string

	_ = filepath.WalkDir(s.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == s.rootDir {
			return nil
		}

		// Only check hourly leaf directories (depth == 4: YYYY/MM/DD/HH)
		rel, relErr := filepath.Rel(s.rootDir, path)
		if relErr != nil {
			return nil
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 4 {
			return nil
		}

		// Check if any data files remain in this partition
		hasData := false
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			return nil
		}
		for _, entry := range entries {
			name := entry.Name()
			if strings.HasSuffix(name, ".zst") || strings.HasSuffix(name, ".log") {
				hasData = true
				break
			}
		}

		if !hasData {
			delete(s.partitionSummaries, path)
			// Remove orphaned index files
			for _, idxName := range []string{indexJSONLFileName, indexJSONFileName} {
				idxPath := filepath.Join(path, idxName)
				if info, statErr := os.Stat(idxPath); statErr == nil {
					s.totalBytes -= info.Size()
					_ = os.Remove(idxPath)
				}
			}
			// Remove any remaining temp files
			for _, entry := range entries {
				_ = os.Remove(filepath.Join(path, entry.Name()))
			}
			emptyDirs = append(emptyDirs, path)
		}
		return nil
	})

	// Remove empty directories bottom-up (deepest first so parent removal works)
	sort.Slice(emptyDirs, func(i, j int) bool {
		return emptyDirs[i] > emptyDirs[j]
	})
	for _, dir := range emptyDirs {
		_ = os.Remove(dir)
		// Try to clean up empty parent directories up to rootDir
		for parent := filepath.Dir(dir); parent != s.rootDir; parent = filepath.Dir(parent) {
			if err := os.Remove(parent); err != nil {
				break // Directory not empty; stop ascending
			}
		}
	}
}

// CompactPartition consolidates multiple small blocks in a partition directory
// into larger, highly compressed Zstandard blocks, resolving the small-file problem.
// It atomically rewrites index.jsonl and unlinks superseded blocks.
// Returns the count of blocks reduced and bytes freed.
func (s *FileStore) CompactPartition(dir string, maxBlockRecords int) (int, int64, error) {
	if maxBlockRecords <= 0 {
		maxBlockRecords = 5000
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, 0, ErrStoreClosed
	}

	headers, ok := s.partitionCache[dir]
	if !ok || len(headers) < 2 {
		diskHeaders, err := LoadPartitionIndex(dir)
		if err != nil || len(diskHeaders) < 2 {
			return 0, 0, nil
		}
		headers = diskHeaders
	}

	var allEntries []logentry.LogEntry
	var oldFiles []string
	var oldBytes int64

	for _, h := range headers {
		filePath := filepath.Join(dir, h.FileName)
		oldFiles = append(oldFiles, filePath)
		oldBytes += h.CompressedBytes

		compressed, err := os.ReadFile(filePath)
		if err != nil {
			return 0, 0, fmt.Errorf("read block %s for compaction: %w", h.FileName, err)
		}
		entries, err := DecodeBlock(compressed)
		if err != nil {
			return 0, 0, fmt.Errorf("decode block %s for compaction: %w", h.FileName, err)
		}
		allEntries = append(allEntries, entries...)
	}

	if len(allEntries) == 0 {
		return 0, 0, nil
	}

	var newHeaders []BlockHeader
	var newFiles []string
	var newBytes int64

	blockIdx := 0
	for i := 0; i < len(allEntries); i += maxBlockRecords {
		end := i + maxBlockRecords
		if end > len(allEntries) {
			end = len(allEntries)
		}
		chunk := allEntries[i:end]

		header, payload, err := EncodeBlock(chunk)
		if err != nil {
			for _, nf := range newFiles {
				_ = os.Remove(nf)
			}
			return 0, 0, fmt.Errorf("encode compacted chunk: %w", err)
		}

		header.FileName = fmt.Sprintf("compact_%s_%04d.zst", header.ID[:8], blockIdx)
		newFilePath := filepath.Join(dir, header.FileName)

		if err := os.WriteFile(newFilePath, payload, 0o600); err != nil {
			for _, nf := range newFiles {
				_ = os.Remove(nf)
			}
			return 0, 0, fmt.Errorf("write compacted block %s: %w", newFilePath, err)
		}

		newFiles = append(newFiles, newFilePath)
		newHeaders = append(newHeaders, header)
		newBytes += header.CompressedBytes
		blockIdx++
	}

	if err := RewritePartitionIndex(dir, newHeaders, s.syncOnWrite); err != nil {
		for _, nf := range newFiles {
			_ = os.Remove(nf)
		}
		return 0, 0, fmt.Errorf("rewrite compacted index: %w", err)
	}

	for _, oldFile := range oldFiles {
		_ = os.Remove(oldFile)
	}

	s.partitionCache[dir] = newHeaders
	s.partitionSummaries[dir] = s.buildPartitionSummaryLocked(dir, newHeaders)
	freedBytes := oldBytes - newBytes
	s.totalBytes -= freedBytes

	blocksReduced := len(headers) - len(newHeaders)
	return blocksReduced, freedBytes, nil
}

// Compact scans all partition directories and merges blocks in partitions with at least minBlocks.
// Returns total blocks reduced and total bytes freed.
func (s *FileStore) Compact(ctx context.Context, minBlocks int) (int, int64, error) {
	if minBlocks < 2 {
		minBlocks = 2
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, 0, ErrStoreClosed
	}

	var candidateDirs []string
	for dir, headers := range s.partitionCache {
		if len(headers) >= minBlocks {
			candidateDirs = append(candidateDirs, dir)
		}
	}
	s.mu.RUnlock()

	var totalReduced int
	var totalFreed int64

	for _, dir := range candidateDirs {
		if ctx.Err() != nil {
			return totalReduced, totalFreed, ctx.Err()
		}
		reduced, freed, err := s.CompactPartition(dir, 5000)
		if err != nil {
			return totalReduced, totalFreed, err
		}
		totalReduced += reduced
		totalFreed += freed
	}

	return totalReduced, totalFreed, nil
}

// Close closes the file store.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
