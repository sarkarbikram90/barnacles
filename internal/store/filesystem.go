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

// FileStore persists log entries into time-segmented JSON Lines files.
type FileStore struct {
	rootDir     string
	syncOnWrite bool

	mu          sync.RWMutex
	closed      bool
	totalBytes  int64
	knownHosts  map[string]struct{}
	knownSrcs   map[string]struct{}
	activeFiles map[string]*os.File
}

// Compile-time interface check.
var _ LogStore = (*FileStore)(nil)

// Config defines filesystem storage options.
type Config struct {
	Directory   string
	SyncOnWrite bool
}

// NewFileStore initializes the filesystem storage directory and index.
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
		activeFiles: make(map[string]*os.File),
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
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".log") {
			info, err := d.Info()
			if err == nil {
				total += info.Size()
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

// Append appends a batch of log entries to the appropriate time-segmented files.
func (s *FileStore) Append(ctx context.Context, entries []logentry.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	// Group entries by target segment file: YYYY/MM/DD/hour.log
	grouped := make(map[string][][]byte)

	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		ts := entry.Timestamp
		if ts.IsZero() {
			ts = time.Now().UTC()
		}

		subDir := filepath.Join(
			s.rootDir,
			ts.Format("2006"),
			ts.Format("01"),
			ts.Format("02"),
		)
		fileName := fmt.Sprintf("%s.log", ts.Format("15"))
		filePath := filepath.Join(subDir, fileName)

		data, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		data = append(data, '\n')

		grouped[filePath] = append(grouped[filePath], data)

		if entry.Host != "" {
			s.knownHosts[entry.Host] = struct{}{}
		}
		if entry.Source != "" {
			s.knownSrcs[entry.Source] = struct{}{}
		}
	}

	for targetPath, byteList := range grouped {
		dir := filepath.Dir(targetPath)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("create segment directory %q: %w", dir, err)
		}

		f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("open segment file %q: %w", targetPath, err)
		}

		var written int64
		for _, b := range byteList {
			n, writeErr := f.Write(b)
			if n > 0 {
				written += int64(n)
			}
			if writeErr != nil {
				_ = f.Close()
				s.totalBytes += written
				return fmt.Errorf("write entry to %q: %w", targetPath, writeErr)
			}
		}

		if s.syncOnWrite {
			_ = f.Sync()
		}
		_ = f.Close()

		s.totalBytes += written
	}

	return nil
}

// Query searches stored log files for entries matching the filter criteria.
func (s *FileStore) Query(ctx context.Context, q logentry.Query) ([]logentry.LogEntry, error) {
	q.Normalize()

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	s.mu.RUnlock()

	var matched []logentry.LogEntry

	// Collect candidate log files sorted by modification time or path
	files, err := s.collectLogFiles()
	if err != nil {
		return nil, fmt.Errorf("collect log files: %w", err)
	}

	// Read in reverse order (newest to oldest) so user sees recent logs first
	for i := len(files) - 1; i >= 0; i-- {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if len(matched) >= q.Limit {
			break
		}

		filePath := files[i]
		if err := s.queryFile(ctx, filePath, q, &matched); err != nil {
			continue
		}
	}

	return matched, nil
}

func (s *FileStore) collectLogFiles() ([]string, error) {
	var files []string
	err := filepath.WalkDir(s.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".log") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func (s *FileStore) queryFile(ctx context.Context, path string, q logentry.Query, matched *[]logentry.LogEntry) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	scanner := bufio.NewScanner(f)
	// Support long lines up to 1MB
	scanBuf := make([]byte, 64*1024)
	scanner.Buffer(scanBuf, 1024*1024)

	var fileEntries []logentry.LogEntry

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry logentry.LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		if entry.Matches(q) {
			fileEntries = append(fileEntries, entry)
		}
	}

	// Reverse file entries so latest are first
	for j := len(fileEntries) - 1; j >= 0; j-- {
		if len(*matched) >= q.Limit {
			break
		}
		*matched = append(*matched, fileEntries[j])
	}

	return scanner.Err()
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

// Prune removes old files exceeding maxAge or maxSizeBytes.
func (s *FileStore) Prune(ctx context.Context, maxAge time.Duration, maxSizeBytes int64) (int, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, 0, ErrStoreClosed
	}

	files, err := s.collectLogFiles()
	if err != nil {
		return 0, 0, err
	}

	var deleted int
	var freed int64
	now := time.Now()

	for _, path := range files {
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
