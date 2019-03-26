package log

import (
	"fmt"
	"os"
)

func fileExists(name string) (bool, error) {
	info, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if info.IsDir() {
		return false, fmt.Errorf("log: directory found at %s", name)
	}
	return true, nil
}

func createFile(name string, size int64, contents []byte) (err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}
	defer func() {
		e := f.Close()
		if err != nil {
			err = e
		}
		if err != nil {
			_ = os.Remove(name)
			err = fmt.Errorf("log: createFile %s: %v", name, err)
		}
	}()
	if _, err = f.Write(contents); err != nil {
		return
	}
	if err = f.Truncate(size); err != nil {
		return
	}
	err = f.Sync()
	return
}

// mmapFile ---------------------------------------------------

type mmapFile struct {
	*os.File
	data []byte
}

func (f *mmapFile) size() int64 {
	return int64(len(f.data))
}

func (f *mmapFile) Close() error {
	err := munmap(f.data)
	if e := f.File.Close(); err == nil {
		err = e
	}
	return err
}

func (f *mmapFile) readUint64(off uint64) uint64 {
	return byteOrder.Uint64(f.data[off:])
}

func (f *mmapFile) writeUint64(v uint64, off int64) error {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, v)
	_, err := f.WriteAt(b, off)
	return err
}

func openFile(name string) (*mmapFile, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("log: openFile %s: %v", name, err)
	}

	info, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	data, err := mmap(f, int(info.Size()))
	if err != nil {
		return nil, err
	}
	return &mmapFile{f, data}, nil
}
