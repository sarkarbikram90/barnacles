package tailer_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/tailer"
)

func TestGlobToRegexp(t *testing.T) {
	tests := []struct {
		pattern    string
		matches    []string
		mismatches []string
	}{
		{
			pattern: "logs/*.log",
			matches: []string{"logs/app.log", "logs/error.log"},
			mismatches: []string{
				"logs/sub/app.log",
				"logs/app.txt",
				"other/app.log",
			},
		},
		{
			pattern: "logs/**/*.log",
			matches: []string{
				"logs/app.log",
				"logs/sub/app.log",
				"logs/sub/nested/app.log",
			},
			mismatches: []string{
				"logs/app.txt",
				"logs/sub/app.json",
				"other/app.log",
			},
		},
		{
			pattern:    "*.log",
			matches:    []string{"app.log", "server.log"},
			mismatches: []string{"sub/app.log", "app.txt"},
		},
	}

	for _, tc := range tests {
		re, _, err := tailer.GlobToRegexp(tc.pattern)
		if err != nil {
			t.Fatalf("GlobToRegexp failed for %q: %v", tc.pattern, err)
		}

		for _, m := range tc.matches {
			if !re.MatchString(m) {
				t.Errorf("pattern %q should match %q", tc.pattern, m)
			}
		}
		for _, mm := range tc.mismatches {
			if re.MatchString(mm) {
				t.Errorf("pattern %q should NOT match %q", tc.pattern, mm)
			}
		}
	}
}

func TestTailerManager_DynamicDiscovery(t *testing.T) {
	tempDir := t.TempDir()
	subDir := filepath.Join(tempDir, "services", "auth")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	file1 := filepath.Join(tempDir, "root.log")
	file2 := filepath.Join(subDir, "auth.log")

	if err := os.WriteFile(file1, []byte("root line 1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, []byte("auth line 1\n"), 0644); err != nil {
		t.Fatal(err)
	}

	pattern := filepath.ToSlash(filepath.Join(tempDir, "**/*.log"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr, err := tailer.NewManager(ctx, tailer.ManagerConfig{
		Pattern:            pattern,
		PollInterval:       100 * time.Millisecond,
		TailerPollInterval: 20 * time.Millisecond,
		StartPosition:      "beginning",
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	if mgr.ActiveCount() != 2 {
		t.Fatalf("expected 2 active files initially, got %d (paths: %v)", mgr.ActiveCount(), mgr.ActivePaths())
	}

	// Read initial lines
	received := make(map[string]bool)
	deadline := time.After(2 * time.Second)
	for len(received) < 2 {
		select {
		case lineEvt := <-mgr.Lines():
			received[lineEvt.Text] = true
		case err := <-mgr.Errors():
			t.Logf("manager error: %v", err)
		case <-deadline:
			t.Fatalf("timeout waiting for initial lines, received: %v", received)
		}
	}

	if !received["root line 1"] || !received["auth line 1"] {
		t.Errorf("expected lines not received: %v", received)
	}

	// Dynamically create a new file matching the glob
	file3 := filepath.Join(tempDir, "services", "payment.log")
	if err := os.WriteFile(file3, []byte("payment line 1\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Wait for manager to discover file3 and stream its line
	discovered := false
	discoverDeadline := time.After(3 * time.Second)
	for !discovered {
		select {
		case lineEvt := <-mgr.Lines():
			if lineEvt.Text == "payment line 1" {
				discovered = true
			}
		case <-discoverDeadline:
			t.Fatalf("timeout waiting for dynamic file line, active files: %v", mgr.ActivePaths())
		}
	}

	if mgr.ActiveCount() != 3 {
		t.Errorf("expected 3 active files after dynamic addition, got %d", mgr.ActiveCount())
	}

	// Now delete file1 and verify manager cleans it up
	if err := os.Remove(file1); err != nil {
		t.Fatalf("remove file1: %v", err)
	}

	// Allow rescan ticker to process deletion
	cleanupDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(cleanupDeadline) {
		if mgr.ActiveCount() == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if mgr.ActiveCount() != 2 {
		t.Errorf("expected 2 active files after deletion, got %d", mgr.ActiveCount())
	}
}
