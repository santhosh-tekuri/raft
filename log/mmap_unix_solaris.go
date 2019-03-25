package log

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func mmap(file *os.File, sz int) ([]byte, error) {
	// Map the data file to memory.
	b, err := unix.Mmap(int(file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	if err := unix.Madvise(b, syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %s", err)
	}

	return b, nil
}

// munmap unmaps a DB's data file from memory.
func munmap(b []byte) error {
	return unix.Munmap(b)
}
