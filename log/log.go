package log

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
)

// ErrNotFound is the error returned by Get and GetN when requested
// entry does not exist in log.
var ErrNotFound = errors.New("log: entry not found")

// Options contains necessary configuration for Log.
type Options struct {
	// FileMode used for creating and opening files.
	FileMode os.FileMode

	// MaxSegmentEntries is max number of entries allowed
	// in a segment. This applies only to newly created
	// indexes. Index file is pre-allocated based on this.
	MaxSegmentEntries int

	// MaxSegmentSize is the maximum size of segment file.
	// This applies only to newly created segments. Segment
	// file is pre-allocated based on this.
	MaxSegmentSize int
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

// LastIndex returns index of last entry.
// Note that there will be no entry at this
// index if Count is zero.
func (l *Log) LastIndex() uint64 {
	return l.last.lastIndex()
}

// Count returns number of entries available in log.
// Note that Count changes when you do removeLTE
// or removeGTE.
func (l *Log) Count() uint64 {
	if l.last.prev == nil {
		return l.last.idx.n
	}
	return (l.last.off - l.first.off) + l.last.idx.n
}

// segment returns the segment where the given index
// lies. Note that this method does not check lastIndex.
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

// Sync flushes any newly appended entries to disk.
//
// Sync is automatically called by removeLTE, removeGTE and Close.
// Append does not call Sync. if process/system crashes any newly
// appended entries after last Sync will be lost.
//
// It is recommended to call Sync after batch of Appends.
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

// RemoveGTE removes all entries greater than or equal to
// given index.
//
// If i>lastIndex this does nothing and does not return error.
// If i<=lastIndex after this operation, lastIndex becomes max(i-1, 0).
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

// RemoveLTE hints to remove all entries less than or equal to
// given index. It might not remove all requested entries.
//
// This does not change lastIndex of log.
// If i>=lastIndex, all entries are removed and count becomes zero.
// If i<lastIndex, it removes only segments whose lastIndex<=i.
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

// Close syncs and closes the log.
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
