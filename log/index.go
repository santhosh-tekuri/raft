package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

var byteOrder = binary.LittleEndian

type index struct {
	n   int
	off []byte
	f   *os.File
}

func newIndex(file string, cap int) (*index, error) {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		if err := ioutil.WriteFile(file, make([]byte, 8), 0600); err != nil {
			return nil, fmt.Errorf("log: init index file : %v", err)
		}
	}
	info, err := os.Stat(file)
	if err != nil {
		return nil, fmt.Errorf("log: stat index: %v", err)
	}
	n := int(info.Size()/8 - 1)
	if n > cap {
		cap = n
	}

	f, err := os.OpenFile(file, os.O_RDWR, 644)
	if err != nil {
		return nil, fmt.Errorf("log: open index: %v", err)
	}
	off := make([]byte, (cap+1)*8, (cap+1)*8)
	if _, err := io.ReadFull(f, off[0:(n+1)*8]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("log: read index: %v", err)
	}
	return &index{n, off, f}, nil
}

func (idx *index) cap() int {
	return len(idx.off)/8 - 1
}

func (idx *index) dataSize() int64 {
	return idx.offset(idx.n)
}

func (idx *index) offset(i int) int64 {
	return int64(byteOrder.Uint64(idx.off[8*i:]))
}

func (idx *index) isFull() bool {
	return idx.n == idx.cap()
}

func (idx *index) append(off int64) error {
	s := (idx.n + 1) * 8
	b := idx.off[s : s+8]
	byteOrder.PutUint64(b, uint64(off))
	if err := writeAt(idx.f, b, int64(s)); err != nil {
		return fmt.Errorf("log: write index: %v", err)
	}
	idx.n++
	return nil
}

func (idx *index) truncate(n int) error {
	if n < 0 || n > idx.n {
		panic("log: invalid truncate size")
	}
	if n < idx.n {
		if err := idx.f.Truncate(int64((n + 1) * 8)); err != nil {
			return fmt.Errorf("log: truncate index: %v", err)
		}
		if err := idx.f.Sync(); err != nil {
			return fmt.Errorf("log: sync index: %v", err)
		}
		idx.n = n
	}
	return nil
}

func (idx *index) close() error {
	return idx.f.Close()
}

func (idx *index) remove() error {
	return os.Remove(idx.f.Name())
}

// helpers ------------------------------------------------

func writeAt(f *os.File, b []byte, off int64) error {
	if _, err := f.WriteAt(b, off); err != nil {
		return err
	}
	return f.Sync()
}
