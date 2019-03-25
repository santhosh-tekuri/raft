package log

import (
	"syscall"
)

// fdatasync flushes written data to a file descriptor.
func (f *mmapFile) syncData() error {
	return syscall.Fdatasync(int(f.Fd()))
}
