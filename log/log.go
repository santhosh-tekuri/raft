package log

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("log: entry not found")

type Options struct {
	fileMode          os.FileMode
	MaxSegmentEntries int
	MaxSegmentSize    int
}

func (o Options) validate() error {
	if o.MaxSegmentEntries <= 0 {
		return fmt.Errorf("log: maxSegmentEntries is <=0")
	}
	maxInt := int(^uint(0) >> 1)
	maxCap := indexCap(maxInt)
	if o.MaxSegmentEntries > maxCap {
		return fmt.Errorf("log: maxSegmentEntries is >%d", maxCap)
	}

	if o.MaxSegmentSize <= 0 {
		return fmt.Errorf("log: maxSegmentSize is <=0")
	}
	return nil
}

// -------------------------------------------------------------------------------

// Log is storage abstraction of an append-only,
// totally ordered sequence of records ordered by time.
//
// Log entries start from zero index. The entry is opaque
// []byte
type Log struct {
	dir   string
	first *segment
	last  *segment
	opt   Options
}

// Open creates and opens log at the given path.
// If the dir does not exist then it will be created automatically.
func Open(dir string, dirMode os.FileMode, opt Options) (*Log, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	first, last, err := openSegments(dir, opt)
	if err != nil {
		for last != nil {
			_ = last.close()
			last = last.prev
		}
		return nil, err
	}

	return &Log{
		dir:   dir,
		first: first,
		last:  last,
		opt:   opt,
	}, nil
}

func (l *Log) LastIndex() uint64 {
	return l.last.lastIndex()
}

func (l *Log) Count() uint64 {
	if l.last.prev == nil {
		return l.last.idx.n
	}
	return (l.last.off - l.first.off) + l.last.idx.n
}

func (l *Log) segment(i uint64) *segment {
	s := l.last
	for s != nil {
		if i >= s.off {
			if i >= s.off+s.idx.cap {
				return nil
			}
			return s
		}
		s = s.prev
	}
	return nil
}

func (l *Log) Get(i uint64) ([]byte, error) {
	s := l.segment(i)
	if s == nil {
		return nil, ErrNotFound
	}
	return s.get(i, 1), nil
}

func (l *Log) GetN(i, n uint64) ([][]byte, error) {
	s := l.segment(i)
	if s == nil {
		return nil, ErrNotFound
	}
	if n > 1 && l.segment(i+n-1) == nil {
		return nil, ErrNotFound
	}
	var buffs [][]byte
	for n > 0 {
		sn := s.idx.cap - (i - s.off)
		if sn > n {
			sn = n
		}
		buffs = append(buffs, s.get(i, sn))
		n -= sn
		i += sn
		s = s.next
	}
	return buffs, nil
}

// todo: handle the case where entry size is > maxSegmentSize

func (l *Log) Append(d []byte) (err error) {
	if l.last.isFull(len(d)) {
		s, err := newSegment(l.dir, l.last.lastIndex()+1, l.opt)
		if err != nil {
			return err
		}
		s.dirty = l.last.dirty
		connect(l.last, s)
		l.last = s
	}
	return l.last.append(d)
}

func (l *Log) Sync() error {
	s := l.last
	for s != nil && s.idx.dirty {
		if err := s.sync(); err != nil {
			return err
		}
		s = s.prev
	}
	return nil
}

func (l *Log) RemoveGTE(i uint64) error {
	if err := l.Sync(); err != nil {
		return err
	}
	var err error
	for {
		if l.last.off > i { // remove it
			prev := l.last.prev
			if prev == nil {
				prev, err = newSegment(l.dir, i, l.opt)
				if err != nil {
					return err
				}
				l.first = prev
			} else {
				disconnect(prev, l.last)
			}
			_ = l.last.close()
			_ = l.last.remove()
			l.last = prev
		} else if l.last.off <= i { // do rtrim
			return l.last.removeGTE(i)
		}
	}
}

func (l *Log) RemoveLTE(i uint64) error {
	if err := l.Sync(); err != nil {
		return err
	}
	var err error
	for {
		if l.first.idx.n > 0 && l.first.lastIndex() <= i {
			next := l.first.next
			if next == nil {
				next, err = newSegment(l.dir, l.first.lastIndex()+1, l.opt)
				if err != nil {
					_ = removeSegment(l.dir, l.first.lastIndex()+1)
					return err
				}
				l.last = next
			} else {
				disconnect(l.first, next)
			}
			_ = l.first.close()
			_ = l.first.remove()
			l.first = next
		} else {
			return nil
		}
	}
}

func (l *Log) Close() error {
	err := l.Sync()
	s := l.last
	for s != nil {
		if e := s.close(); err == nil {
			err = e
		}
		s = s.prev
	}
	return err
}

// helpers --------------------------------------------------------

func openSegments(dir string, opt Options) (first, last *segment, err error) {
	offs, err := indexes(dir)
	if err != nil {
		return
	}
	if len(offs) == 0 {
		offs = append(offs, 0)
	}

	if first, err = newSegment(dir, offs[0], opt); err != nil {
		return
	}
	last = first
	offs = offs[1:]

	var s *segment
	for _, off := range offs {
		if last.idx.n > 0 && off == last.lastIndex()+1 {
			if s, err = newSegment(dir, off, opt); err != nil {
				return
			}
			connect(last, s)
			last = s
			offs = offs[1:]
		} else {
			// dangling segment: remove it
			if err = removeSegment(dir, off); err == nil {
				return
			}
		}
	}
	return
}

// linked list ----------------------------------------------

func connect(s1, s2 *segment) {
	s1.next = s2
	s2.prev = s1
}

func disconnect(s1, s2 *segment) {
	s1.next = nil
	s2.prev = nil
}
