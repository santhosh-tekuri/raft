package log

import (
	"fmt"
	"os"
	"path/filepath"
)

type segment struct {
	off     uint64
	idx     *index
	f       *os.File
	maxSize int64
	prev    *segment
	dirty   bool
}

func newSegment(dir string, off uint64, cap uint64, maxSize int64) (*segment, error) {
	file := filepath.Join(dir, fmt.Sprintf("%d.log", off))
	exists, err := fileExists(file)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err = createFile(file, maxSize, nil); err != nil {
			return nil, err
		}
	}

	// fix maxSize if necessary
	info, err := os.Stat(file)
	if err != nil {
		return nil, fmt.Errorf("log: stat %s: %v", file, err)
	}
	maxSize = info.Size()

	f, err := openFile(file)
	if err != nil {
		return nil, err
	}

	idx, err := newIndex(filepath.Join(dir, fmt.Sprintf("%d.index", off)), cap)
	if err != nil {
		return nil, err
	}

	return &segment{off: off, idx: idx, f: f, maxSize: maxSize}, nil
}

func (s *segment) lastIndex() uint64 {
	if s.idx.n == 0 {
		if s.off == 0 {
			return 0
		}
		return s.off - 1
	}
	return s.off + (s.idx.n - 1)
}

func (s *segment) isFull(newEntrySize int) bool {
	return s.idx.isFull() || s.idx.dataSize+int64(newEntrySize) > s.maxSize
}

func (s *segment) get(i uint64) ([]byte, error) {
	off, n, err := s.idx.entry(i - s.off)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	b := make([]byte, n)
	if err = readFull(s.f, b, off); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *segment) append(b []byte) error {
	if _, err := s.f.WriteAt(b, s.idx.dataSize); err != nil {
		return err
	}
	return s.idx.append(len(b))
}

func (s *segment) sync() error {
	if s.idx.dirty {
		if err := s.f.Sync(); err != nil {
			return err
		}
		return s.idx.sync()
	}
	return nil
}

func (s *segment) close() error {
	err := s.sync()
	if e := s.idx.close(); err == nil {
		err = e
	}
	if e := s.f.Close(); err == nil {
		err = e
	}
	return err
}

func (s *segment) removeGTE(i uint64) error {
	return s.idx.truncate(i - s.off)
}

func (s *segment) remove() error {
	err1 := s.idx.remove()
	err2 := os.Remove(s.f.Name())
	if err1 != nil {
		return err1
	}
	return err2
}
