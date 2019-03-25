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
	dir string
	seg *segment
	opt Options
}

func New(dir string, opt Options) (*Log, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	seg, err := openSegments(dir, opt)
	if err != nil {
		for seg != nil {
			_ = seg.close()
			seg = seg.prev
		}
		return nil, err
	}

	return &Log{
		dir: dir,
		seg: seg,
		opt: opt,
	}, nil
}

func (l *Log) LastIndex() (uint64, error) {
	return l.seg.lastIndex(), nil
}

func (l *Log) Count() (uint64, error) {
	if l.seg.prev == nil {
		return l.seg.idx.n, nil
	}
	s := l.seg
	for s.prev != nil {
		s = s.prev
	}
	first := s.off
	return (l.seg.off - first) + l.seg.idx.n, nil
}

func (l *Log) Get(i uint64) ([]byte, error) {
	s := l.seg
	for s != nil {
		if i >= s.off {
			return s.get(i)
		}
		s = s.prev
	}
	return nil, NotFoundError(i)
}

func (l *Log) Append(d []byte) (err error) {
	if l.seg.isFull(len(d)) {
		s, err := newSegment(l.dir, l.seg.lastIndex()+1, l.opt)
		if err != nil {
			return err
		}
		s.dirty = l.seg.dirty
		s.prev, l.seg = l.seg, s
	}
	return l.seg.append(d)
}

func (l *Log) Sync() error {
	s := l.seg
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
		if l.seg.off > i { // remove it
			prev := l.seg.prev
			if prev == nil {
				prev, err = newSegment(l.dir, i, l.opt)
				if err != nil {
					return err
				}
			} else {
				l.seg.prev = nil
			}
			_ = l.seg.close()
			_ = l.seg.remove()
			l.seg = prev
		} else if l.seg.off <= i { // do rtrim
			return l.seg.removeGTE(i)
		}
	}
}

func (l *Log) RemoveLTE(i uint64) error {
	if err := l.Sync(); err != nil {
		return err
	}
	var err error
	for {
		s, next := l.seg, (*segment)(nil)
		for s.prev != nil {
			s, next = s.prev, s
		}
		// now s is first segment
		if s.idx.n > 0 && s.lastIndex() <= i {
			if next == nil {
				l.seg, err = newSegment(l.dir, s.lastIndex()+1, l.opt)
				if err != nil {
					_ = removeSegment(l.dir, s.lastIndex()+1)
					return err
				}
			} else {
				next.prev = nil
			}
			_ = s.close()
			_ = s.remove()
		} else {
			return nil
		}
	}
}

func (l *Log) Close() error {
	err := l.Sync()
	s := l.seg
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

func openSegments(dir string, opt Options) (*segment, error) {
	offs, err := segments(dir)
	if err != nil {
		return nil, err
	}

	loaded := make(map[uint64]bool)
	seg, err := newSegment(dir, offs[0], opt)
	if err != nil {
		return nil, err
	}
	loaded[offs[0]] = true

	for {
		if seg.idx.n == 0 {
			break
		}
		off := seg.lastIndex() + 1
		exists, err := fileExists(filepath.Join(dir, fmt.Sprintf("%d.index", off)))
		if err != nil {
			return seg, err
		}
		if !exists {
			break
		}
		s, err := newSegment(dir, off, opt)
		if err != nil {
			return seg, err
		}
		s.prev = seg
		seg = s
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
	return seg, err
}

func removeSegment(dir string, off uint64) error {
	index := filepath.Join(dir, fmt.Sprintf("%d.index", off))
	if err := os.RemoveAll(index); err != nil {
		return err
	}
	data := filepath.Join(dir, fmt.Sprintf("%d.log", off))
	return os.RemoveAll(data)
}
