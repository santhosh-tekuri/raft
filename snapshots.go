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

package raft

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// todo: add md5 check

type snapshots struct {
	dir    string
	retain int

	mu    sync.RWMutex
	index uint64
	term  uint64

	usedMu sync.RWMutex
	used   map[uint64]int // map[index]numUses
}

func openSnapshots(dir string, opt Options) (*snapshots, error) {
	if opt.SnapshotsRetain < 1 {
		return nil, fmt.Errorf("raft: must retain at least one snapshot")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	snaps, err := findSnapshots(dir)
	if err != nil {
		return nil, err
	}
	s := &snapshots{
		dir:    dir,
		retain: opt.SnapshotsRetain,
		used:   make(map[uint64]int),
	}
	if len(snaps) > 0 {
		s.index = snaps[0]
		if meta, err := s.meta(); err != nil {
			return nil, err
		} else {
			s.term = meta.term
		}
	}
	return s, nil
}

func (s *snapshots) latest() (index, term uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index, s.term
}

func (s *snapshots) meta() (snapshotMeta, error) {
	if s.index == 0 {
		return snapshotMeta{index: 0, term: 0}, nil
	}
	f, err := os.Open(metaFile(s.dir, s.index))
	if err != nil {
		return snapshotMeta{}, err
	}
	defer f.Close()
	meta := snapshotMeta{}
	return meta, meta.decode(f)
}

func (s *snapshots) applyRetain() error {
	snaps, err := findSnapshots(s.dir)
	if err != nil {
		return err
	}
	s.usedMu.RLock()
	defer s.usedMu.RUnlock()
	for i, index := range snaps {
		if i >= s.retain && s.used[index] == 0 {
			if e := os.Remove(metaFile(s.dir, index)); e == nil {
				if e := os.Remove(snapFile(s.dir, index)); err == nil {
					err = e
				}
			} else if err == nil {
				err = e
			}
		}
	}
	return err
}

// snapshot ----------------------------------------------------

func (s *snapshots) open() (*snapshot, error) {
	meta, err := s.meta()
	if err != nil {
		return nil, err
	}
	file := snapFile(s.dir, meta.index)

	// validate file size
	info, err := os.Stat(file)
	if err != nil {
		return nil, err
	}
	if info.Size() != meta.size {
		return nil, fmt.Errorf("raft: size of %q is %d, want %d", file, info.Size(), meta.size)
	}

	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	s.used[meta.index]++
	return &snapshot{
		snaps: s,
		meta:  meta,
		file:  f,
	}, nil
}

type snapshot struct {
	snaps *snapshots
	meta  snapshotMeta
	file  *os.File
}

func (s *snapshot) release() {
	_ = s.file.Close()
	s.snaps.usedMu.Lock()
	defer s.snaps.usedMu.Unlock()
	if s.snaps.used[s.meta.index] == 1 {
		delete(s.snaps.used, s.meta.index)
	} else {
		s.snaps.used[s.meta.index]--
	}
}

// snapshotSink ----------------------------------------------------

func (s *snapshots) new(index, term uint64, config Config) (*snapshotSink, error) {
	f, err := os.Create(snapFile(s.dir, index))
	if err != nil {
		return nil, err
	}
	return &snapshotSink{
		snaps: s,
		meta:  snapshotMeta{index: index, term: term, config: config},
		file:  f,
	}, nil
}

type snapshotSink struct {
	snaps *snapshots
	meta  snapshotMeta
	file  *os.File
}

func (s *snapshotSink) done(err error) (snapshotMeta, error) {
	if err != nil {
		_ = s.file.Close()
		_ = os.Remove(s.file.Name())
		return s.meta, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(s.file.Name())
		}
	}()
	if err = s.file.Close(); err != nil {
		return s.meta, err
	}
	info, err := os.Stat(s.file.Name())
	if err != nil {
		return s.meta, err
	}
	s.meta.size = info.Size()

	file := filepath.Join(s.snaps.dir, "meta.tmp")
	temp, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return s.meta, err
	}
	defer func() {
		if temp != nil {
			_ = temp.Close()
			_ = os.RemoveAll(temp.Name())
		}
	}()
	if err = s.meta.encode(temp); err != nil {
		return s.meta, err
	}
	if err = temp.Close(); err != nil {
		return s.meta, err
	}
	file = metaFile(s.snaps.dir, s.meta.index)
	if err = os.Rename(temp.Name(), file); err != nil {
		return s.meta, err
	}
	temp = nil
	s.snaps.mu.Lock()
	s.snaps.index, s.snaps.term = s.meta.index, s.meta.term
	s.snaps.mu.Unlock()
	_ = s.snaps.applyRetain() // todo: trace error
	return s.meta, nil
}

// snapshotMeta ----------------------------------------------------

type snapshotMeta struct {
	index  uint64
	term   uint64
	config Config
	size   int64
}

func (m *snapshotMeta) encode(w io.Writer) error {
	if err := writeUint64(w, m.index); err != nil {
		return err
	}
	if err := writeUint64(w, m.term); err != nil {
		return err
	}
	if err := m.config.encode().encode(w); err != nil {
		return err
	}
	return writeUint64(w, uint64(m.size))
}

func (m *snapshotMeta) decode(r io.Reader) (err error) {
	if m.index, err = readUint64(r); err != nil {
		return err
	}
	if m.term, err = readUint64(r); err != nil {
		return err
	}
	e := &entry{}
	if err = e.decode(r); err != nil {
		return err
	}
	if err = m.config.decode(e); err != nil {
		return err
	}
	if size, err := readUint64(r); err != nil {
		return err
	} else {
		m.size = int64(size)
	}
	return nil
}

// helpers ----------------------------------------------------

func metaFile(dir string, index uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%d.meta", index))
}
func snapFile(dir string, index uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%d.snap", index))
}

// findSnapshots returns list of snapshots from latest to oldest
func findSnapshots(dir string) ([]uint64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.meta"))
	if err != nil {
		return nil, err
	}
	var snaps []uint64
	for _, m := range matches {
		m = filepath.Base(m)
		m = strings.TrimSuffix(m, ".meta")
		i, err := strconv.ParseUint(m, 10, 64)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, i)
	}
	sort.Sort(decrUint64Slice(snaps))
	return snaps, nil
}
