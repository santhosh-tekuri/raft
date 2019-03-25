package log

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

// Based on: https://github.com/edsrzf/mmap-go
func mmap(file *os.File, sz int) ([]byte, error) {
	// Open a file mapping handle.
	sizelo := uint32(sz >> 32)
	sizehi := uint32(sz) & 0xffffffff
	h, errno := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, syscall.PAGE_READONLY, sizelo, sizehi, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(sz))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	b := make([]byte, 0)
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bh.Data = addr
	bh.Len = sz
	bh.Cap = bh.Len
	return b, nil
}

// Based on: https://github.com/edsrzf/mmap-go
func munmap(b []byte) error {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	addr := bh.Data
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
