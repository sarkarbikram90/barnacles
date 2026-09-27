//go:build windows

package tailer

import (
	"os"
	"syscall"
)

// getFileIdentity returns the volume serial number (deviceID) and fileIndex (inode equivalent)
// for an opened file on Windows NTFS/ReFS filesystems.
func getFileIdentity(f *os.File, info os.FileInfo) (uint64, uint64, error) {
	if f == nil {
		return 0, 0, os.ErrInvalid
	}
	var data syscall.ByHandleFileInformation
	err := syscall.GetFileInformationByHandle(syscall.Handle(f.Fd()), &data)
	if err != nil {
		return 0, 0, err
	}
	devID := uint64(data.VolumeSerialNumber)
	fileID := (uint64(data.FileIndexHigh) << 32) | uint64(data.FileIndexLow)
	return devID, fileID, nil
}
