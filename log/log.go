package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type NotFoundError uint64

func (e NotFoundError) Error() string {
	return fmt.Sprintf("log: entry %d not found", uint64(e))
}

type Options struct {
	MaxSegmentEntries uint64
	MaxSegmentSize    int64
}

func (o Options) validate() error {
	if o.MaxSegmentEntries <= 0 {
		return fmt.Errorf("log: maxSegmentEntries is <=0")
	}
	if o.MaxSegmentSize <= 0 {
		return fmt.Errorf("log: maxSegmentSize is <=0")
	}
	return nil
}

// -------------------------------------------------------------------------------

type Log struct {
	dir   string
	first *segment
	last  *segment
	opt   Options
}

func New(dir string, opt Options) (*Log, error) {
	if err := opt.validate(); err != nil {
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

func (l *Log) LastIndex() (uint64, error) {
	return l.last.lastIndex(), nil
}

func (l *Log) Count() (uint64, error) {
	if l.last.prev == nil {
		return l.last.idx.n, nil
	}
	return (l.last.off - l.first.off) + l.last.idx.n, nil
}

func (l *Log) Get(i uint64) ([]byte, error) {
	s := l.last
	for s != nil {
		if i >= s.off {
			return s.get(i), nil
		}
		s = s.prev
	}
	return nil, NotFoundError(i)
}

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

func (l *Log) isNotFound(err error) bool {
	_, ok := err.(NotFoundError)
	return ok
}

// helpers --------------------------------------------------------

func segments(dir string) ([]uint64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.index"))
	if err != nil {
		return nil, err
	}
	var offs []uint64
	for _, m := range matches {
		m = strings.TrimSuffix(m, ".index")
		i, err := strconv.ParseUint(m, 10, 64)
		if err != nil {
			return nil, err
		}
		offs = append(offs, i)
	}
	sort.Slice(offs, func(i, j int) bool {
		return offs[i] < offs[j]
	})
	if len(offs) == 0 {
		offs = append(offs, 0)
	}
	return offs, nil
}

func openSegments(dir string, opt Options) (first, last *segment, err error) {
	offs, err := segments(dir)
	if err != nil {
		return
	}

	loaded := make(map[uint64]bool)
	first, err = newSegment(dir, offs[0], opt)
	if err != nil {
		return
	}
	loaded[offs[0]] = true

	last = first
	s, exists := (*segment)(nil), false
	for {
		if last.idx.n == 0 {
			break
		}
		off := last.lastIndex() + 1
		exists, err = fileExists(filepath.Join(dir, fmt.Sprintf("%d.index", off)))
		if err != nil {
			return
		}
		if !exists {
			break
		}
		s, err = newSegment(dir, off, opt)
		if err != nil {
			return
		}
		connect(last, s)
		last = s
		loaded[off] = true
	}

	// remove dangling segments if any
	for _, off := range offs {
		if !loaded[off] {
			if e := removeSegment(dir, off); e != nil {
				err = e
			}
		}
	}
	return
}

func removeSegment(dir string, off uint64) error {
	index := filepath.Join(dir, fmt.Sprintf("%d.index", off))
	if err := os.RemoveAll(index); err != nil {
		return err
	}
	data := filepath.Join(dir, fmt.Sprintf("%d.log", off))
	return os.RemoveAll(data)
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
