package mmap

import (
	"syscall"
)

// fdatasync flushes written data to a file descriptor.
func (f *File) SyncData() error {
	return syscall.Fdatasync(int(f.Fd()))
}

func (f *File) Sync() error {
	return f.File.Sync()
}
