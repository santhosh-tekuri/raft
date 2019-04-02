package mmap

import (
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Based on: https://github.com/edsrzf/mmap-go

type addrinfo struct {
	file     *os.File
	mapview  windows.Handle
	writable bool
}

func openFile(file *os.File, flag int, size int) (*File, error) {
	var prot, access int
	if flag == os.O_RDONLY {
		prot, access = windows.PAGE_READONLY, windows.FILE_MAP_READ
	} else {
		prot, access = windows.PAGE_READWRITE, windows.FILE_MAP_WRITE
	}

	// The maximum size is the area of the file, starting from 0,
	// that we wish to allow to be mappable. It is the sum of
	// the length the user requested, plus the offset where that length
	// is starting from. This does not map the data into memory.
	var off int64 = 0
	maxSizeHigh := uint32((off + int64(size)) >> 32)
	maxSizeLow := uint32((off + int64(size)) & 0xFFFFFFFF)
	h, errno := windows.CreateFileMapping(windows.Handle(file.Fd()), nil, uint32(prot), maxSizeHigh, maxSizeLow, nil)
	if h == 0 {
		_ = file.Close()
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Actually map a view of the data into memory. The view's size
	// is the length the user requested.
	fileOffsetHigh := uint32(off >> 32)
	fileOffsetLow := uint32(off & 0xFFFFFFFF)
	addr, errno := windows.MapViewOfFile(h, uint32(access), fileOffsetHigh, fileOffsetLow, uintptr(size))
	if addr == 0 {
		_ = file.Close()
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	b := make([]byte, 0)
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bh.Data = addr
	bh.Len = size
	bh.Cap = bh.Len

	return &File{
		name: file.Name(),
		Data: b,
		handle: &addrinfo{
			file:     file,
			mapview:  h,
			writable: flag != os.O_RDONLY,
		},
	}, nil
}

func (f *File) Sync() error {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&f.Data))
	errno := windows.FlushViewOfFile(header.Data, uintptr(header.Len))
	if errno != nil {
		return os.NewSyscallError("FlushViewOfFile", errno)
	}
	handle := f.handle.(*addrinfo)
	if handle.writable {
		if err := windows.FlushFileBuffers(windows.Handle(handle.file.Fd())); err != nil {
			return os.NewSyscallError("FlushFileBuffers", err)
		}
	}
	return nil
}

func (f *File) Close() error {
	if err := f.Sync(); err != nil {
		return err
	}
	header := (*reflect.SliceHeader)(unsafe.Pointer(&f.Data))
	if err := windows.UnmapViewOfFile(header.Data); err != nil {
		return err
	}
	handle := f.handle.(*addrinfo)
	if err := windows.CloseHandle(windows.Handle(handle.mapview)); err != nil {
		return err
	}
	return handle.file.Close()
}
