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
	"encoding/binary"
	"os"

	"github.com/santhosh-tekuri/raft/mmap"
)

var byteOrder = binary.LittleEndian

type segment struct {
	prevIndex uint64
	prev      *segment
	next      *segment

	file   *mmap.File // index file
	n      int        // number of entries
	size   int        // log size
	synced int        // number of entries synced, will be -1 on GTE
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
	s := &segment{
		prevIndex: prevIndex,
		file:      file,
	}
	s.n = s.offset(0)
	s.synced = s.n
	s.size = s.offset(s.n + 1)
	return s, nil
}

func (s *segment) at(i int) int {
	return len(s.file.Data) - i*8 - 8
}

func (s *segment) offset(i int) int {
	return int(byteOrder.Uint64(s.file.Data[s.at(i):]))
}

func (s *segment) setOffset(off int, i int) {
	byteOrder.PutUint64(s.file.Data[s.at(i):], uint64(off))
}

func (s *segment) lastIndex() uint64 {
	return s.prevIndex + uint64(s.n)
}

func (s *segment) get(i uint64, n uint64) []byte {
	if i > s.prevIndex {
		i := int(i - s.prevIndex)
		from, to := s.offset(i), s.offset(i+int(n))
		return s.file.Data[from:to]
	}
	panic("i<=prevIndex")
}

func (s *segment) available() int {
	return s.at(s.n+2) - s.size
}

func (s *segment) append(b []byte) {
	copy(s.file.Data[s.size:], b)
	size := s.size + len(b)
	s.setOffset(size, s.n+2)
	s.n, s.size = s.n+1, size
}

func (s *segment) removeGTE(i uint64) error {
	n := int(i - s.prevIndex - 1)
	if n < s.n {
		s.setOffset(n, 0)
		s.n, s.size, s.synced = n, s.offset(n+1), -1
	}
	return s.sync()
}

func (s *segment) dirty() bool {
	return s.synced < s.n
}

func (s *segment) sync() error {
	if s.dirty() {
		if err := s.file.Sync(); err != nil {
			return err
		}
		s.setOffset(s.n, 0)
		if err := s.file.Sync(); err != nil {
			return err
		}
		s.synced = s.n
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
