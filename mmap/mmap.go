package mmap

import (
	"os"
)

type File struct {
	*os.File
	Data []byte
}

func OpenFile(name string, flag int, mode os.FileMode) (*File, error) {
	f, err := os.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	data, err := mmap(f, int(info.Size()))
	if err != nil {
		return nil, err
	}
	return &File{f, data}, nil
}

func (f *File) Size() int64 {
	return int64(len(f.Data))
}

func (f *File) Truncate(size int64) (err error) {
	if err = munmap(f.Data); err != nil {
		return err
	}
	if err = f.File.Truncate(size); err != nil {
		return err
	}
	f.Data, err = mmap(f.File, int(size))
	return err
}

func (f *File) Close() error {
	err := munmap(f.Data)
	if e := f.File.Close(); err == nil {
		err = e
	}
	return err
}
