package mmap

import (
	"os"
)

type File struct {
	name   string
	Data   []byte
	handle interface{}
}

func OpenFile(name string, flag int, mode os.FileMode) (*File, error) {
	f, err := os.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return openFile(f, flag, int(info.Size()))
}

func (f *File) Name() string {
	return f.name
}
