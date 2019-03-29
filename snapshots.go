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
	dir   string
	mu    sync.RWMutex
	index uint64
	term  uint64
}

func (s *snapshots) init() error {
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return err
	}
	snaps, err := findSnapshots(s.dir)
	if err != nil {
		return err
	}
	if len(snaps) > 0 {
		s.index = snaps[0]
		if meta, err := s.meta(); err != nil {
			return err
		} else {
			s.term = meta.Term
		}
	}
	return err
}

func (s *snapshots) latest() (index, term uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index, s.term
}

func (s *snapshots) meta() (SnapshotMeta, error) {
	if s.index == 0 {
		return SnapshotMeta{Index: 0, Term: 0}, nil
	}
	f, err := os.Open(metaFile(s.dir, s.index))
	if err != nil {
		return SnapshotMeta{}, err
	}
	defer f.Close()
	meta := SnapshotMeta{}
	return meta, meta.decode(f)
}

func (s *snapshots) open() (SnapshotMeta, io.ReadCloser, error) {
	meta, err := s.meta()
	f, err := os.Open(snapFile(s.dir, meta.Index))
	return meta, f, err
}

func (s *snapshots) new(index, term uint64, config Config) (*snapshotSink, error) {
	f, err := os.Create(snapFile(s.dir, index))
	if err != nil {
		return nil, err
	}
	return &snapshotSink{
		snaps: s,
		meta:  SnapshotMeta{Index: index, Term: term, Config: config},
		file:  f,
	}, nil
}

// snapshotSink ----------------------------------------------------

type snapshotSink struct {
	snaps *snapshots
	meta  SnapshotMeta
	file  *os.File
}

func (s *snapshotSink) done(err error) (SnapshotMeta, error) {
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
	s.meta.Size = info.Size()

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
	file = metaFile(s.snaps.dir, s.meta.Index)
	if err = os.Rename(temp.Name(), file); err != nil {
		return s.meta, err
	}
	temp = nil
	s.snaps.mu.Lock()
	s.snaps.index, s.snaps.term = s.meta.Index, s.meta.Term
	s.snaps.mu.Unlock()
	return s.meta, nil
}

// snapshotMeta ----------------------------------------------------

type SnapshotMeta struct {
	Index  uint64
	Term   uint64
	Config Config
	Size   int64
}

func (m *SnapshotMeta) encode(w io.Writer) error {
	if err := writeUint64(w, m.Index); err != nil {
		return err
	}
	if err := writeUint64(w, m.Term); err != nil {
		return err
	}
	if err := m.Config.encode().encode(w); err != nil {
		return err
	}
	return writeUint64(w, uint64(m.Size))
}

func (m *SnapshotMeta) decode(r io.Reader) (err error) {
	if m.Index, err = readUint64(r); err != nil {
		return err
	}
	if m.Term, err = readUint64(r); err != nil {
		return err
	}
	e := &entry{}
	if err = e.decode(r); err != nil {
		return err
	}
	if err = m.Config.decode(e); err != nil {
		return err
	}
	if size, err := readUint64(r); err != nil {
		return err
	} else {
		m.Size = int64(size)
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