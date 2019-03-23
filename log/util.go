package log

import (
	"fmt"
	"io"
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

func dirExists(name string) (bool, error) {
	info, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("log: file found at %s", name)
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
			err = fmt.Errorf("log: createFile %s: %v", name, err)
		}
	}()
	if _, err = f.Write(contents); err != nil {
		return
	}
	if err = f.Truncate(size); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	return
}

func openFile(name string) (*os.File, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("log: openFile %s: %v", name, err)
	}
	return f, nil
}

func writeAt(f *os.File, b []byte, off int64) error {
	if _, err := f.WriteAt(b, off); err != nil {
		return err
	}
	return f.Sync()
}

func readUint64(f *os.File, off int64) (uint64, error) {
	b := make([]byte, 8)
	if err := readFull(f, b, off); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(b), nil
}

func writeUint64(f *os.File, v uint64, off int64) error {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, v)
	return writeAt(f, b, off)
}

func readFull(f *os.File, b []byte, off int64) error {
	r := io.NewSectionReader(f, off, int64(len(b)))
	_, err := io.ReadFull(r, b)
	return err
}
