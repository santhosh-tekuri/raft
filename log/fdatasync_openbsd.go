package log

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
func (f *mmapFile) syncData() error {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&f.data))
	addr := bh.Data
	sz := int(f.size())
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, addr, uintptr(sz), msInvalidate)
	if errno != 0 {
		return errno
	}
	return f.File.Sync()
}
