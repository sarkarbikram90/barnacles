// Package tailer provides robust file tailing with support for append-only writes,
// partial line buffering, EOF waiting, file truncation, log file rotation,
// and dynamic multi-file glob pattern discovery.
package tailer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// DiscoveredLine represents a line fanned in from a dynamically discovered log file.
type DiscoveredLine struct {
	Path string
	Text string
}

// ManagerConfig defines configuration parameters for TailerManager.
type ManagerConfig struct {
	Pattern            string        // Glob pattern e.g. "/var/log/**/*.log"
	PollInterval       time.Duration // Rescan interval for directory changes (default: 500ms)
	TailerPollInterval time.Duration // File polling interval for active tailers (default: 50ms)
	StartPosition      string        // "beginning" or "end" (default: "end")
	MaxLineBytes       int           // Maximum bytes per line (default: 1MB)
	CheckpointRegistry *CheckpointRegistry
}

// TailerManager dynamically discovers files matching a glob pattern (including **),
// instantiates individual tailers, monitors file additions and deletions, and
// multiplexes all ingested lines into a single stream.
type TailerManager struct {
	cfg           ManagerConfig
	baseDir       string
	regex         *regexp.Regexp
	linesCh       chan DiscoveredLine
	errCh         chan error
	activeMu      sync.RWMutex
	activeTailers map[string]*managedTailer
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	closeOnce     sync.Once
}

type managedTailer struct {
	path   string
	tailer *Tailer
	cancel context.CancelFunc
}

// NewManager creates and starts a dynamic TailerManager for the specified glob pattern.
func NewManager(ctx context.Context, cfg ManagerConfig) (*TailerManager, error) {
	if strings.TrimSpace(cfg.Pattern) == "" {
		return nil, errors.New("glob pattern cannot be empty")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.TailerPollInterval <= 0 {
		cfg.TailerPollInterval = defaultPollInterval
	}
	if cfg.MaxLineBytes <= 0 {
		cfg.MaxLineBytes = defaultMaxLineBytes
	}
	if cfg.StartPosition == "" {
		cfg.StartPosition = "end"
	}

	re, baseDir, err := GlobToRegexp(cfg.Pattern)
	if err != nil {
		return nil, fmt.Errorf("compile glob pattern %q: %w", cfg.Pattern, err)
	}

	mgrCtx, cancel := context.WithCancel(ctx)
	m := &TailerManager{
		cfg:           cfg,
		baseDir:       baseDir,
		regex:         re,
		linesCh:       make(chan DiscoveredLine, channelBufferSize*2),
		errCh:         make(chan error, 32),
		activeTailers: make(map[string]*managedTailer),
		ctx:           mgrCtx,
		cancel:        cancel,
	}

	// Perform initial discovery scan synchronously
	_ = m.scanAndUpdate()

	m.wg.Add(1)
	go m.scannerLoop()

	return m, nil
}

// Lines returns the multiplexed receive-only channel for all discovered lines.
func (m *TailerManager) Lines() <-chan DiscoveredLine {
	return m.linesCh
}

// Errors returns the channel yielding tailing warnings or errors.
func (m *TailerManager) Errors() <-chan error {
	return m.errCh
}

// ActiveCount returns the current number of actively tailed files.
func (m *TailerManager) ActiveCount() int {
	m.activeMu.RLock()
	defer m.activeMu.RUnlock()
	return len(m.activeTailers)
}

// ActivePaths returns a sorted slice of currently tailed file paths.
func (m *TailerManager) ActivePaths() []string {
	m.activeMu.RLock()
	defer m.activeMu.RUnlock()
	paths := make([]string, 0, len(m.activeTailers))
	for p := range m.activeTailers {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

// Scan returns all existing files currently matching the glob pattern.
func (m *TailerManager) Scan() ([]string, error) {
	var matches []string

	if _, err := os.Stat(m.baseDir); os.IsNotExist(err) {
		return matches, nil
	}

	cleanBase := filepath.Clean(m.baseDir)
	err := filepath.WalkDir(cleanBase, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // Gracefully skip unreadable paths
		}
		if d.IsDir() {
			return nil
		}

		slashPath := filepath.ToSlash(path)
		slashPath = strings.TrimPrefix(slashPath, "./")
		if m.regex.MatchString(slashPath) {
			matches = append(matches, path)
		}
		return nil
	})

	return matches, err
}

func (m *TailerManager) scannerLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			_ = m.scanAndUpdate()
		}
	}
}

func (m *TailerManager) scanAndUpdate() error {
	matches, err := m.Scan()
	if err != nil {
		m.emitError(fmt.Errorf("scan error: %w", err))
		return err
	}

	foundSet := make(map[string]bool, len(matches))
	for _, path := range matches {
		clean := filepath.Clean(path)
		foundSet[clean] = true
	}

	m.activeMu.Lock()
	defer m.activeMu.Unlock()

	// 1. Remove deleted files or files no longer matching
	for activePath, mt := range m.activeTailers {
		if !foundSet[activePath] {
			// Double check if file exists
			if _, err := os.Stat(activePath); os.IsNotExist(err) {
				mt.cancel()
				_ = mt.tailer.Close()
				delete(m.activeTailers, activePath)
			}
		}
	}

	// 2. Add new files
	for _, path := range matches {
		clean := filepath.Clean(path)
		if _, exists := m.activeTailers[clean]; exists {
			continue
		}

		tCtx, tCancel := context.WithCancel(m.ctx)
		tCfg := Config{
			Path:               clean,
			StartPosition:      m.cfg.StartPosition,
			PollInterval:       m.cfg.TailerPollInterval,
			MaxLineBytes:       m.cfg.MaxLineBytes,
			CheckpointRegistry: m.cfg.CheckpointRegistry,
		}

		t, err := New(tCtx, tCfg)
		if err != nil {
			tCancel()
			m.emitError(fmt.Errorf("start tailer for %s: %w", clean, err))
			continue
		}

		mt := &managedTailer{
			path:   clean,
			tailer: t,
			cancel: tCancel,
		}
		m.activeTailers[clean] = mt

		m.wg.Add(1)
		go m.fanIn(tCtx, mt)
	}

	return nil
}

func (m *TailerManager) fanIn(ctx context.Context, mt *managedTailer) {
	defer m.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-mt.tailer.Errors():
			if !ok {
				return
			}
			m.emitError(err)
		case line, ok := <-mt.tailer.Lines():
			if !ok {
				return
			}
			select {
			case m.linesCh <- DiscoveredLine{Path: mt.path, Text: line}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (m *TailerManager) emitError(err error) {
	select {
	case m.errCh <- err:
	default:
	}
}

// Close gracefully terminates all active tailers, stops the scanner, and waits for routines to exit.
func (m *TailerManager) Close() error {
	m.closeOnce.Do(func() {
		m.cancel()

		m.activeMu.Lock()
		for _, mt := range m.activeTailers {
			mt.cancel()
			_ = mt.tailer.Close()
		}
		m.activeTailers = make(map[string]*managedTailer)
		m.activeMu.Unlock()

		m.wg.Wait()
		close(m.linesCh)
		close(m.errCh)
	})
	return nil
}

// GlobToRegexp converts a glob pattern (supporting *, ?, and recursive **) into a compiled
// regular expression and extracts the static base directory prefix for directory walking.
func GlobToRegexp(pattern string) (*regexp.Regexp, string, error) {
	norm := filepath.ToSlash(filepath.Clean(pattern))
	norm = strings.TrimPrefix(norm, "./")

	// Determine static base directory
	parts := strings.Split(norm, "/")
	var baseParts []string
	foundWildcard := false
	for _, p := range parts {
		if !foundWildcard && !strings.ContainsAny(p, "*?[]{}") {
			baseParts = append(baseParts, p)
		} else {
			foundWildcard = true
		}
	}

	baseDir := strings.Join(baseParts, "/")
	if baseDir == "" {
		baseDir = "."
	}
	if strings.HasPrefix(pattern, "/") && !strings.HasPrefix(baseDir, "/") {
		baseDir = "/" + baseDir
	}

	var sb strings.Builder
	sb.WriteString("^")
	i := 0
	for i < len(norm) {
		if i+3 <= len(norm) && norm[i:i+3] == "/**" {
			if i+3 == len(norm) {
				sb.WriteString("(?:/.*)?")
				i += 3
				continue
			} else if norm[i+3] == '/' {
				sb.WriteString("(?:/.+)?")
				i += 3
				continue
			}
		}
		if i+2 <= len(norm) && norm[i:i+2] == "**" {
			sb.WriteString(".*")
			i += 2
			continue
		}

		c := norm[i]
		switch c {
		case '*':
			sb.WriteString("[^/]*")
		case '?':
			sb.WriteString("[^/]")
		case '.', '+', '(', ')', '|', '^', '$', '[', ']', '{', '}':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		default:
			sb.WriteByte(c)
		}
		i++
	}
	sb.WriteString("$")

	re, err := regexp.Compile(sb.String())
	if err != nil {
		return nil, baseDir, err
	}
	return re, baseDir, nil
}
