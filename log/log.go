// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"errors"
	"fmt"
	"os"
)

// ErrNotFound is returned by Get, GetN if the entry index is <=PrevIndex.
var ErrNotFound = errors.New("log: entry not found")

// ErrExceedsSegmentSize is returned by Append, when size of given entry
// does not fit in empty segment.
var ErrExceedsSegmentSize = errors.New("log: entry exceeds segment size")

// Options contains necessary configuration.
type Options struct {
	FileMode    os.FileMode
	SegmentSize int
}

func (o Options) validate() error {
	if o.FileMode.String()[1] != 'r' {
		return fmt.Errorf("log: FileMode %q has no read permission", o.FileMode)
	}
	if o.SegmentSize < 1024 {
		return fmt.Errorf("log: SegmentSize %d is too smal", o.SegmentSize)
	}
	return nil
}

// Log implements append only list of entries persisted to disk.
type Log struct {
	dir string
	opt Options

	first *segment
	last  *segment
	index []uint64 // for view: index[0] is prevIndex, index[1] is lastIndex
}

// Open opens log from given directory. if dir does not exist it is created
// with given dirMode.
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
			_ = first.close()
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

// ViewAt create a view with bounds [prevIndex, lastIndex]. View is
// safer to use in another goroutine while Appending in another
// goroutine. Only reading methods should be used in the view.
func (l *Log) ViewAt(prevIndex, lastIndex uint64) *Log {
	if lastIndex > l.LastIndex() {
		panic(fmt.Sprintf("log: %d>lastIndex(%d)", lastIndex, l.LastIndex()))
	}
	if prevIndex > lastIndex || prevIndex < l.PrevIndex() {
		return nil
	}
	s := l.last
	for {
		if prevIndex >= s.prevIndex {
			break
		}
		s = s.prev
	}
	return &Log{
		dir:   l.dir,
		opt:   l.opt,
		first: s,
		last:  l.segment(lastIndex),
		index: []uint64{prevIndex, lastIndex},
	}
}

// View is equivalent to ViewAt(PrevIndex, LastIndex)
func (l *Log) View() *Log {
	return l.ViewAt(l.PrevIndex(), l.LastIndex())
}

// PrevIndex returns prevIndex in log.
// log starts from PrevIndex+1.
func (l *Log) PrevIndex() uint64 {
	if l.index != nil {
		return l.index[0]
	}
	return l.first.prevIndex
}

// LastIndex returns lastIndex in log.
// If Count() is zero, there is no entry at i.
func (l *Log) LastIndex() uint64 {
	if l.index != nil {
		return l.index[1]
	}
	return l.last.lastIndex()
}

// Count returns number of entries in log.
func (l *Log) Count() uint64 {
	return l.LastIndex() - l.PrevIndex()
}

func (l *Log) segment(i uint64) *segment {
	if i > l.LastIndex() {
		panic(fmt.Sprintf("log: %d>lastIndex(%d)", i, l.LastIndex()))
	}
	if i <= l.PrevIndex() {
		return nil
	}

	s := l.last
	for {
		if i > s.prevIndex {
			return s
		}
		if s == l.first {
			return nil
		}
		s = s.prev
	}
}

// Contains returns true if index i exists in log.
// Index exists in log if it is >PrevIndex and <=LastIndex.
func (l *Log) Contains(i uint64) bool {
	return i > l.PrevIndex() && i <= l.LastIndex()
}

// Get returns i-th entry. It is equivalent to GetN(i, 1).
// The returned []byte is mmapped data. It can be used as long as
// Close, RemoveLTE, RemoveGTE is not called. Any of these three calls
// might invalidate the data returned and further use of it will
// cause errors.
//
// if index is >LastIndex it panics. If index <PrevIndex, it returns
// ErrNotFound.
func (l *Log) Get(i uint64) ([]byte, error) {
	s := l.segment(i)
	if s == nil {
		return nil, ErrNotFound
	}
	return s.get(i, 1), nil
}

// GetN returns n entries from i. that is entries i, i+1,...,i+n-1.
// The returned [][]byte is mmapped data. It returns one []byte per
// segment file. The returned data can be used as long as Close,
// RemoveLTE, RemoveGTE is not called. Any of these three calls
// might invalidate the data returned and further use of it will
// cause errors.
//
// if index is >LastIndex it panics. If index <PrevIndex, it returns
// ErrNotFound.
func (l *Log) GetN(i uint64, n uint64) ([][]byte, error) {
	if i+(n-1) > l.LastIndex() {
		panic(fmt.Sprintf("log: %d>lastIndex(%d)", i+(n-1), l.LastIndex()))
	}
	s := l.segment(i)
	if s == nil {
		return nil, ErrNotFound
	}
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

// Append appends an entry to log. the param []byte
// is opaque to Log and is not interpreted.
func (l *Log) Append(b []byte) error {
	if l.last.available() < len(b) {
		if l.last.n == 0 {
			return ErrExceedsSegmentSize
		}
		if len(b) > l.opt.SegmentSize-3*8 {
			l.opt.SegmentSize = len(b) + 3*8
		}
		if err := l.Commit(); err != nil {
			return err
		}
		s, err := openSegment(l.dir, l.LastIndex(), l.opt)
		if err != nil {
			return err
		}
		connect(l.last, s)
		l.last = s
	}
	l.last.append(b)
	return nil
}

// CanLTE tells which entries will be removed if RemoveLTE(i)
// is called. You can only remove segment completely or not.
// for example if log has two segments, with first segment
// entries 1 to 100, second segment from 101 t0 150, then
// you can remove entries <=100, that is CanLTE(110)=100,
// CanLTE(50)=0
func (l *Log) CanLTE(i uint64) uint64 {
	s := l.first
	for s != l.last {
		if s.n > 0 && s.lastIndex() <= i {
			s = s.next
		} else {
			break
		}
	}
	return s.prevIndex
}

// RemoveLTE requests to remove all entries <=i. It removes
// a segment completely or not. So it might not remove all
// entries <=i. You can use CanLTE to check which entries
// can be removed. Before removing entries it implicitly
// commits the log.
func (l *Log) RemoveLTE(i uint64) error {
	if err := l.Commit(); err != nil {
		return err
	}
	for l.first != l.last {
		if l.first.n > 0 && l.first.lastIndex() <= i {
			s := l.first
			l.first = l.first.next
			disconnect(l.first.prev, l.first)
			if err := s.closeAndRemove(); err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

// RemoveGTE removes all entries >=i from log.
// Before removing entries it implicitly commits the log.
func (l *Log) RemoveGTE(i uint64) error {
	if err := l.Commit(); err != nil {
		return err
	}
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

// Reset clears all entries and resets to given lastIndex
func (l *Log) Reset(lastIndex uint64) error {
	// remove all segments
	for l.first != nil {
		if err := l.first.closeAndRemove(); err != nil {
			return err
		}
		l.first = l.first.next
	}

	s, err := openSegment(l.dir, lastIndex, l.opt)
	if err != nil {
		return err
	}
	l.first, l.last = s, s
	return nil
}

// CommitN commits at least n entries to stable storage.
func (l *Log) CommitN(n uint64) error {
	for s := l.last; s != nil; s = s.prev {
		if !s.dirty() {
			break
		} else if s.prevIndex >= n {
			continue
		} else if err := s.sync(); err != nil {
			return err
		}
	}
	return nil
}

// Commit commits all entries to stable storage.
func (l *Log) Commit() error {
	return l.CommitN(l.LastIndex())
}

// Close commits all entries and closes the log.
func (l *Log) Close() error {
	err := l.Commit()
	for s := l.last; s != nil; s = s.prev {
		if e := s.close(); err == nil {
			err = e
		}
	}
	return err
}
