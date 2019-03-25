package log

import (
	"encoding/binary"
	"fmt"
	"os"
)

var byteOrder = binary.LittleEndian

type index struct {
	cap      uint64
	n        uint64
	dataSize int64
	f        *os.File
}

func newIndex(file string, cap uint64) (*index, error) {
	exists, err := fileExists(file)
	if err != nil {
		return nil, err
	}
	if !exists {
		fileSize := (int64(cap) + 2) * 8
		if err = createFile(file, fileSize, make([]byte, 16)); err != nil {
			return nil, err
		}
	}

	// fix cap if necessary
	info, err := os.Stat(file)
	if err != nil {
		return nil, fmt.Errorf("log: stat %s: %v", file, err)
	}
	if fcap := uint64(info.Size()/8 - 2); fcap > cap {
		cap = fcap
	}

	f, err := openFile(file)
	if err != nil {
		return nil, err
	}
	n, err := readUint64(f, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	idx := &index{cap: cap, n: n, f: f}
	idx.dataSize, err = idx.offset(idx.n)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return idx, nil
}

func (idx *index) entry(i uint64) (int64, int, error) {
	off, err := idx.offset(i)
	if err != nil {
		return 0, 0, err
	}
	limit, err := idx.offset(i + 1)
	if err != nil {
		return 0, 0, err
	}
	if limit < off {
		return off, 0, nil
	}
	return off, int(limit - off), nil
}

func (idx *index) offset(i uint64) (int64, error) {
	off, err := readUint64(idx.f, int64((i+1)*8))
	if err != nil {
		return 0, err
	}
	return int64(off), err
}

func (idx *index) isFull() bool {
	return idx.n == idx.cap
}

func (idx *index) append(newEntrySize int) error {
	off := idx.dataSize + int64(newEntrySize)
	if err := writeUint64(idx.f, uint64(off), int64((idx.n+2)*8)); err != nil {
		return err
	}
	if err := writeUint64(idx.f, idx.n+1, 0); err != nil {
		return err
	}
	idx.n++
	idx.dataSize = off
	return nil
}

func (idx *index) truncate(n uint64) error {
	if n >= 0 && n < idx.n {
		if err := writeUint64(idx.f, n, 0); err != nil {
			return err
		}
		off, err := idx.offset(n)
		if err != nil {
			return err
		}
		idx.n = n
		idx.dataSize = off
	}
	return nil
}

func (idx *index) close() error {
	return idx.f.Close()
}

func (idx *index) remove() error {
	return os.Remove(idx.f.Name())
}
