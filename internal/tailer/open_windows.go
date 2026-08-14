//go:build windows

package tailer

import (
	"os"
	"syscall"
)

// openForTailing opens a file on Windows with FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
// to permit rename-based rotation by other processes while the tailer has the file open.
func openForTailing(path string) (*os.File, os.FileInfo, error) {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, nil, err
	}

	handle, err := syscall.CreateFile(
		pathPtr,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, nil, err
	}

	f := os.NewFile(uintptr(handle), path)
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	return f, info, nil
}
