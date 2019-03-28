package mmap

import (
	"reflect"
	"syscall"
	"unsafe"
)

const (
	msAsync      = 1 << iota // perform asynchronous writes
	msSync                   // perform synchronous writes
	msInvalidate             // invalidate cached data
)

// Based on: https://github.com/boltdb/bolt
func msync(b []byte) error {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	addr := bh.Data
	sz := len(b)
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, addr, uintptr(sz), msInvalidate)
	if errno != 0 {
		return errno
	}
	return nil
}

func (f *File) SyncData() error {
	if err := msync(f.Data); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *File) Sync() error {
	if err := msync(f.Data); err != nil {
		return err
	}
	return f.File.Sync()
}
