package log

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

var ErrNotFound = errors.New("log: entry not found")
var ErrExceedsSegmentSize = errors.New("log: entry exceeds segment size")

type Options struct {
	FileMode    os.FileMode
	SegmentSize int
}

func (o Options) validate() error {
	if o.SegmentSize < 1024 {
		return fmt.Errorf("log: SegmentSize %d is too smal", o.SegmentSize)
	}
	return nil
}

type Log struct {
	dir string
	opt Options

	mu    sync.RWMutex
	first *segment
	last  *segment
}

func Open(dir string, dirMode os.FileMode, opt Options) (*Log, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	first, last, err := openSegments(dir, opt)
	if err != nil {
		for first != nil {
			if e := first.close(); err == nil {
				err = e
			}
			first = first.next
		}
		return nil, err
	}

	return &Log{
		dir:   dir,
		opt:   opt,
		first: first,
		last:  last,
	}, nil
}

func (l *Log) PrevIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.first.prevIndex
}

func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.last.lastIndex()
}

func (l *Log) Count() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.last.lastIndex() - l.first.prevIndex
}

func (l *Log) segment(i uint64) *segment {
	l.mu.RLock()
	s := l.last
	for s != nil {
		if i > s.prevIndex {
			break
		}
		s = s.prev
	}
	l.mu.RUnlock()
	return s
}

func (l *Log) Get(i uint64) ([]byte, error) {
	s := l.segment(i)
	if s == nil {
		return nil, ErrNotFound
	}
	return s.get(i, 1), nil
}

func (l *Log) GetN(i uint64, n uint64) ([][]byte, error) {
	s := l.segment(i)
	if s == nil {
		return nil, ErrNotFound
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	var buffs [][]byte
	for n > 0 {
		if s == l.last {
			buffs = append(buffs, s.get(i, n))
			break
		} else {
			sn := s.lastIndex() - (i - 1)
			if sn > n {
				sn = n
			}
			buffs = append(buffs, s.get(i, sn))
			i += sn
			n -= sn
			s = s.next
		}
	}
	return buffs, nil
}

func (l *Log) Append(b []byte) error {
	if l.last.available() < len(b) {
		if l.last.n == 0 {
			return ErrExceedsSegmentSize
		}
		if len(b) > l.opt.SegmentSize-3*8 {
			l.opt.SegmentSize = len(b) + 3*8
		}
		s, err := openSegment(l.dir, l.LastIndex(), l.opt)
		if err != nil {
			return err
		}
		s.dirty = l.last.dirty
		l.mu.Lock()
		connect(l.last, s)
		l.last = s
		l.mu.Unlock()
	}
	return l.last.append(b)
}

func (l *Log) RemoveLTE(i uint64) error {
	if err := l.Sync(); err != nil {
		return err
	}
	lastIndex := l.LastIndex()
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		if l.first.n > 0 && l.first.lastIndex() <= i {
			s := l.first
			l.first = l.first.next
			if l.first != nil {
				disconnect(l.first.prev, l.first)
			}
			if err := s.closeAndRemove(); err != nil {
				return err
			}
			if l.first == nil {
				s, err := openSegment(l.dir, lastIndex, l.opt)
				if err != nil {
					return err
				}
				l.first, l.last = s, s
				break
			}
		} else {
			break
		}
	}
	return nil
}

func (l *Log) RemoveGTE(i uint64) error {
	if err := l.Sync(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		if i <= l.last.prevIndex+1 {
			if l.last == l.first && i == l.last.prevIndex+1 {
				return l.last.removeGTE(l.last.prevIndex + 1) // clear all entries
			}

			// remove l.last
			s := l.last
			l.last = l.last.prev
			if l.last != nil {
				disconnect(l.last, l.last.next)
			}
			if err := s.closeAndRemove(); err != nil {
				return err
			}

			if l.last == nil {
				if i > 0 {
					i--
				}
				s, err := openSegment(l.dir, i, l.opt)
				if err != nil {
					return err
				}
				l.first, l.last = s, s
				break
			}
		} else if i > l.last.prevIndex {
			if i > l.last.lastIndex() {
				i = l.last.lastIndex() + 1
			}
			return l.last.removeGTE(i)
		} else {
			break
		}
	}
	return nil
}

func (l *Log) Sync() error {
	for s := l.last; s != nil; s = s.prev {
		if !s.dirty {
			break
		} else if err := s.sync(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Close() error {
	err := l.Sync()
	for s := l.last; s != nil; s = s.prev {
		if e := s.close(); err == nil {
			err = e
		}
	}
	return err
}
