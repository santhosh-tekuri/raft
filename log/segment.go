package log

import (
	"fmt"
	"os"
	"path/filepath"
)

type segment struct {
	off   uint64
	idx   *index
	f     *mmapFile
	prev  *segment
	next  *segment
	dirty bool
}

func newSegment(dir string, off uint64, opt Options) (*segment, error) {
	file := segmentFile(dir, off)
	exists, err := fileExists(file)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err = createFile(file, opt.MaxSegmentSize, nil); err != nil {
			return nil, err
		}
	}

	f, err := openFile(file)
	if err != nil {
		return nil, err
	}

	idx, err := newIndex(indexFile(dir, off), opt.MaxSegmentEntries)
	if err != nil {
		return nil, err
	}

	return &segment{off: off, idx: idx, f: f}, nil
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
	return s.idx.isFull() || s.idx.dataSize+int64(newEntrySize) > s.f.size()
}

func (s *segment) get(i uint64, n uint64) []byte {
	i -= s.off
	from, to := s.idx.offset(i), s.idx.offset(i+n)
	if to < from {
		return nil
	}
	return s.f.data[from:to]
}

func (s *segment) append(b []byte) error {
	if _, err := s.f.WriteAt(b, s.idx.dataSize); err != nil {
		return err
	}
	return s.idx.append(len(b))
}

func (s *segment) sync() error {
	if s.idx.dirty {
		if err := s.f.syncData(); err != nil {
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

// helpers -------------------------------------

func segmentFile(dir string, off uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%d.log", off))
}

func removeSegment(dir string, off uint64) error {
	if err := os.RemoveAll(indexFile(dir, off)); err != nil {
		return err
	}
	return os.RemoveAll(segmentFile(dir, off))
}
