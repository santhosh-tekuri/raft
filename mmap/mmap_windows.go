// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mmap

import (
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Based on: https://github.com/edsrzf/mmap-go

type winHandle struct {
	file     *os.File
	mapping  windows.Handle
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
	mapping, errno := windows.CreateFileMapping(windows.Handle(file.Fd()), nil, uint32(prot), maxSizeHigh, maxSizeLow, nil)
	if mapping == 0 {
		_ = file.Close()
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Actually map a view of the data into memory. The view's size
	// is the length the user requested.
	fileOffsetHigh := uint32(off >> 32)
	fileOffsetLow := uint32(off & 0xFFFFFFFF)
	addr, errno := windows.MapViewOfFile(mapping, uint32(access), fileOffsetHigh, fileOffsetLow, uintptr(size))
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
		handle: &winHandle{
			file:     file,
			mapping:  mapping,
			writable: flag != os.O_RDONLY,
		},
	}, nil
}

// Sync commits the current contents of the file to stable storage.
func (f *File) Sync() error {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&f.Data))
	errno := windows.FlushViewOfFile(header.Data, uintptr(header.Len))
	if errno != nil {
		return os.NewSyscallError("FlushViewOfFile", errno)
	}
	handle := f.handle.(*winHandle)
	if handle.writable {
		if err := windows.FlushFileBuffers(windows.Handle(handle.file.Fd())); err != nil {
			return os.NewSyscallError("FlushFileBuffers", err)
		}
	}
	return nil
}

// Close closes the File, rendering it unusable for I/O.
func (f *File) Close() error {
	if err := f.Sync(); err != nil {
		return err
	}
	header := (*reflect.SliceHeader)(unsafe.Pointer(&f.Data))
	if err := windows.UnmapViewOfFile(header.Data); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	handle := f.handle.(*winHandle)
	if err := windows.CloseHandle(handle.mapping); err != nil {
		return os.NewSyscallError("CloseHandle", err)
	}
	return handle.file.Close()
}
