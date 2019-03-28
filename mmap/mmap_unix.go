// +build !windows,!plan9,!solaris

package mmap

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func mmap(file *os.File, sz int) ([]byte, error) {
	// Map the data file to memory.
	b, err := syscall.Mmap(int(file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %s", err)
	}
	return b, nil
}

func munmap(b []byte) error {
	return syscall.Munmap(b)
}

// NOTE: This function is copied from stdlib because it is not available on darwin.
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
