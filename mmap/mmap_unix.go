// +build darwin dragonfly freebsd linux openbsd solaris netbsd

package mmap

import (
	"os"

	"golang.org/x/sys/unix"
)

func openFile(file *os.File, flag int, size int) (*File, error) {
	defer file.Close()
	var prot int
	if flag == os.O_RDONLY {
		prot = unix.PROT_READ
	} else if flag == os.O_WRONLY {
		prot = unix.PROT_WRITE
	} else if flag == os.O_RDWR {
		prot = unix.PROT_READ | unix.PROT_WRITE
	}
	b, err := unix.Mmap(int(file.Fd()), 0, size, prot, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &File{name: file.Name(), Data: b}, nil
}

func (f *File) Sync() error {
	return unix.Msync(f.Data, unix.MS_SYNC)
}

func (f File) Close() error {
	return unix.Munmap(f.Data)
}
