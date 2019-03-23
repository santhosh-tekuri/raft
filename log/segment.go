package log

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type segment struct {
	off uint64
	idx *index
	f   *os.File

	prevMu sync.RWMutex
	prev   *segment
}

func newSegment(dir string, off uint64, cap int) (*segment, error) {
	idx, err := newIndex(filepath.Join(dir, fmt.Sprintf("%d.index", off)), cap)
	if err != nil {
		return nil, err
	}
	file := filepath.Join(dir, fmt.Sprintf("%d.data", off))
	if _, err := os.Stat(file); os.IsNotExist(err) {
		if err := ioutil.WriteFile(file, nil, 0600); err != nil {
			return nil, err
		}
	}
	info, err := os.Stat(file)
	if err != nil {
		return nil, err
	}
	if info.Size() < idx.dataSize() {
		return nil, errors.New("log: dataSize smaller than expected")
	}
	f, err := os.OpenFile(file, os.O_RDWR, 644)
	if err != nil {
		_ = idx.close()
		return nil, err
	}
	return &segment{off: off, idx: idx, f: f}, nil
}

func (s *segment) get(i uint64) ([]byte, error) {
	j := int(i - s.off)
	off := s.idx.offset(j)
	n := s.idx.offset(j+1) - off
	if n <= 0 {
		return nil, nil
	}
	d := make([]byte, n)
	r := io.NewSectionReader(s.f, off, n)
	_, err := io.ReadFull(r, d)
	return d, err
}

func (s *segment) append(d []byte) error {
	if s.idx.isFull() {
		panic("log: index is full")
	}
	off := s.idx.dataSize()
	if err := writeAt(s.f, d, off); err != nil {
		return err
	}
	return s.idx.append(off + int64(len(d)))
}

func (s *segment) close() error {
	if err := s.idx.close(); err != nil {
		return err
	}
	_ = s.f.Close()
	return nil
}

func (s *segment) removeGTE(i uint64) error {
	return s.idx.truncate(int(i - uint64(s.off)))
}

func (s *segment) remove() error {
	err1 := s.idx.remove()
	err2 := os.Remove(s.f.Name())
	if err1 != nil {
		return err1
	}
	return err2
}
