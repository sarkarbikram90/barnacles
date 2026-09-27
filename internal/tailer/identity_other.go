//go:build !windows

package tailer

import (
	"errors"
	"os"
	"syscall"
)

// getFileIdentity returns the device ID and inode number for an opened file on Unix-like filesystems.
func getFileIdentity(f *os.File, info os.FileInfo) (uint64, uint64, error) {
	if info == nil && f != nil {
		var err error
		info, err = f.Stat()
		if err != nil {
			return 0, 0, err
		}
	}
	if info == nil {
		return 0, 0, errors.New("no file info available")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("cannot obtain Stat_t")
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}
