package log

import (
	"encoding/binary"
	"os"

	"github.com/santhosh-tekuri/raft/mmap"
)

var byteOrder = binary.LittleEndian

type segment struct {
	prevIndex uint64
	prev      *segment
	next      *segment

	file  *mmap.File // index file
	n     uint64     // number of entries
	size  int64      // log size
	dirty bool       // is sync needed ?
}

func openSegment(dir string, prevIndex uint64, opt Options) (*segment, error) {
	f := segmentFile(dir, prevIndex)
	if exists, err := fileExists(f); err != nil {
		return nil, err
	} else if !exists {
		if err = createSegment(f, opt); err != nil {
			return nil, err
		}
	}
	file, err := mmap.OpenFile(f, os.O_RDWR, opt.FileMode)
	if err != nil {
		return nil, err
	}
	s := &segment{prevIndex: prevIndex, file: file}
	s.n = uint64(s.offset(0))
	s.size = int64(s.offset(s.n + 1))
	return s, nil
}

func (s *segment) at(i uint64) int64 {
	return s.file.Size() - int64(i*8) - 8
}

func (s *segment) offset(i uint64) int {
	return int(byteOrder.Uint64(s.file.Data[s.at(i):]))
}

func (s *segment) setOffset(v uint64, i uint64) error {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, v)
	_, err := s.file.WriteAt(b, s.at(i))
	return err
}

func (s *segment) lastIndex() uint64 {
	return s.prevIndex + s.n
}

func (s *segment) get(i uint64, n uint64) []byte {
	if i <= s.prevIndex {
		panic("i<=prevIndex")
	}
	i -= s.prevIndex
	from, to := s.offset(i), s.offset(i+n)
	return s.file.Data[from:to]
}

func (s *segment) available() int {
	return int(s.at(s.n+2) - s.size)
}

func (s *segment) append(b []byte) error {
	if _, err := s.file.WriteAt(b, s.size); err != nil {
		return err
	}
	size := s.size + int64(len(b))
	if err := s.setOffset(uint64(size), s.n+2); err != nil {
		return err
	}
	s.n, s.size, s.dirty = s.n+1, size, true
	return nil
}

func (s *segment) removeGTE(i uint64) error {
	n := i - s.prevIndex - 1
	if n < s.n {
		if err := s.setOffset(n, 0); err != nil {
			return err
		}
		s.n, s.size, s.dirty = n, int64(s.offset(n+1)), true
	}
	return s.sync()
}

func (s *segment) sync() error {
	if s.dirty {
		if err := s.file.SyncData(); err != nil {
			return err
		}
		if err := s.setOffset(s.n, 0); err != nil {
			return err
		}
		if err := s.file.SyncData(); err != nil {
			return err
		}
		s.dirty = false
	}
	return nil
}

func (s *segment) close() error {
	err := s.sync()
	if e := s.file.Close(); err == nil {
		err = e
	}
	return err
}

func (s *segment) remove() error {
	return os.Remove(s.file.Name())
}

func (s *segment) closeAndRemove() error {
	err1 := s.close()
	err2 := s.remove()
	if err1 != nil {
		return err1
	}
	return err2
}
