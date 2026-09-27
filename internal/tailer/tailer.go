// Package tailer provides robust file tailing with support for append-only writes,
// partial line buffering, EOF waiting, file truncation, and log file rotation.
package tailer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

var (
	// ErrClosed is returned when operations are attempted on a closed tailer.
	ErrClosed = errors.New("tailer is closed")
	// ErrLineTooLong indicates a line exceeded the maximum configured buffer size.
	ErrLineTooLong = errors.New("log line exceeds maximum allowed size")
)

const (
	defaultPollInterval = 50 * time.Millisecond
	defaultMaxLineBytes = 1024 * 1024 // 1MB
	channelBufferSize   = 128
)

// Config defines the tailer operational parameters.
type Config struct {
	Path               string
	StartPosition      string // "beginning" or "end"
	PollInterval       time.Duration
	MaxLineBytes       int
	CheckpointRegistry *CheckpointRegistry
}

// Tailer watches and streams new lines appended to a target log file.
type Tailer struct {
	cfg       Config
	linesCh   chan string
	errCh     chan error
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New creates and starts a Tailer for the specified configuration.
func New(ctx context.Context, cfg Config) (*Tailer, error) {
	if cfg.Path == "" {
		return nil, errors.New("file path cannot be empty")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.MaxLineBytes <= 0 {
		cfg.MaxLineBytes = defaultMaxLineBytes
	}

	tailCtx, cancel := context.WithCancel(ctx)
	t := &Tailer{
		cfg:     cfg,
		linesCh: make(chan string, channelBufferSize),
		errCh:   make(chan error, 16),
		ctx:     tailCtx,
		cancel:  cancel,
	}

	t.wg.Add(1)
	go t.run()

	return t, nil
}

// Lines returns the receive-only channel yielding parsed log lines.
func (t *Tailer) Lines() <-chan string {
	return t.linesCh
}

// Errors returns the receive-only channel yielding tailer warnings or errors.
func (t *Tailer) Errors() <-chan error {
	return t.errCh
}

// Close stops the tailer gracefully, waiting for the worker goroutine to terminate.
func (t *Tailer) Close() error {
	t.closeOnce.Do(func() {
		t.cancel()
		t.wg.Wait()
		close(t.linesCh)
		close(t.errCh)
	})
	return nil
}

func (t *Tailer) emitError(err error) {
	select {
	case t.errCh <- err:
	default:
	}
}

func (t *Tailer) recordCheckpoint(devID, fileID uint64, offset int64) {
	if t.cfg.CheckpointRegistry != nil && (devID != 0 || fileID != 0) {
		t.cfg.CheckpointRegistry.Set(Checkpoint{
			DeviceID:          devID,
			FileIdentity:      fileID,
			FilePath:          t.cfg.Path,
			ByteOffset:        offset,
			LastSeenTimestamp: time.Now().UTC(),
		})
	}
}

func (t *Tailer) run() {
	defer t.wg.Done()

	var (
		file       *os.File
		fileInfo   os.FileInfo
		devID      uint64
		fileID     uint64
		offset     int64
		lineBuf    bytes.Buffer
		isInitial  = true
		pollTicker = time.NewTicker(t.cfg.PollInterval)
	)
	defer pollTicker.Stop()
	defer func() {
		if file != nil {
			t.recordCheckpoint(devID, fileID, offset)
			_ = file.Close()
		}
	}()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-pollTicker.C:
		}

		// If no file currently opened, attempt to open
		if file == nil {
			f, info, err := t.openFile()
			if err != nil {
				t.emitError(fmt.Errorf("open file %q error: %w", t.cfg.Path, err))
				continue
			}
			file = f
			fileInfo = info
			devID, fileID, _ = getFileIdentity(file, fileInfo)

			if isInitial {
				isInitial = false
				// Check if persistent checkpoint exists
				resumed := false
				if t.cfg.CheckpointRegistry != nil {
					if devID != 0 && fileID != 0 {
						if cp, exists := t.cfg.CheckpointRegistry.Get(devID, fileID); exists {
							if cp.ByteOffset <= info.Size() {
								offset = cp.ByteOffset
							} else {
								offset = 0
							}
							resumed = true
						}
					} else {
						// Only use path fallback when filesystem doesn't provide file identity
						if cp, exists := t.cfg.CheckpointRegistry.GetByPath(t.cfg.Path); exists && cp.ByteOffset <= info.Size() {
							offset = cp.ByteOffset
							resumed = true
						}
					}
				}

				if resumed {
					if _, err := file.Seek(offset, io.SeekStart); err != nil {
						t.emitError(fmt.Errorf("seek to checkpoint offset %d: %w", offset, err))
					}
				} else {
					if t.cfg.StartPosition == "end" {
						offset = info.Size()
						if _, err := file.Seek(offset, io.SeekStart); err != nil {
							t.emitError(fmt.Errorf("seek to end: %w", err))
						}
					} else {
						offset = 0
					}
				}
				t.recordCheckpoint(devID, fileID, offset)
			} else {
				// New file opened after rotation / recreation
				offset = 0
				t.recordCheckpoint(devID, fileID, offset)
			}
		}

		// Check rotation or truncation
		currentInfo, err := os.Stat(t.cfg.Path)
		if err == nil {
			// Check if file was rotated (different file identity)
			if !os.SameFile(fileInfo, currentInfo) {
				// Read remaining old file to EOF before switching
				t.drainFile(file, &offset, &lineBuf)
				t.recordCheckpoint(devID, fileID, offset)
				_ = file.Close()
				file = nil
				continue
			}

			// Check truncation (current size < our read offset)
			if currentInfo.Size() < offset {
				offset = 0
				t.recordCheckpoint(devID, fileID, offset)
				if _, err := file.Seek(0, io.SeekStart); err != nil {
					t.emitError(fmt.Errorf("seek to start on truncation: %w", err))
				}
			}
		}

		// Read new data available from current file
		prevOffset := offset
		t.readAvailable(file, &offset, &lineBuf)
		if offset != prevOffset {
			t.recordCheckpoint(devID, fileID, offset)
		}
	}
}

func (t *Tailer) openFile() (*os.File, os.FileInfo, error) {
	return openForTailing(t.cfg.Path)
}

func (t *Tailer) drainFile(f *os.File, offset *int64, lineBuf *bytes.Buffer) {
	for {
		readBytes := t.readChunk(f, offset, lineBuf)
		if readBytes == 0 {
			break
		}
	}
	// If a partial line remains at EOF of rotated file, emit it
	if lineBuf.Len() > 0 {
		line := lineBuf.String()
		lineBuf.Reset()
		select {
		case t.linesCh <- line:
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *Tailer) readAvailable(f *os.File, offset *int64, lineBuf *bytes.Buffer) {
	for {
		readBytes := t.readChunk(f, offset, lineBuf)
		if readBytes == 0 {
			break
		}
	}
}

func (t *Tailer) readChunk(f *os.File, offset *int64, lineBuf *bytes.Buffer) int {
	buf := make([]byte, 32*1024)
	n, err := f.ReadAt(buf, *offset)
	if n > 0 {
		*offset += int64(n)
		t.processBytes(buf[:n], lineBuf)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		t.emitError(fmt.Errorf("read file chunk: %w", err))
	}
	return n
}

func (t *Tailer) processBytes(data []byte, lineBuf *bytes.Buffer) {
	for _, b := range data {
		if b == '\n' {
			line := lineBuf.String()
			lineBuf.Reset()
			// Remove trailing CR if present
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			if len(line) > 0 {
				select {
				case t.linesCh <- line:
				case <-t.ctx.Done():
					return
				}
			}
		} else {
			if lineBuf.Len() >= t.cfg.MaxLineBytes {
				t.emitError(ErrLineTooLong)
				lineBuf.Reset()
			} else {
				lineBuf.WriteByte(b)
			}
		}
	}
}
