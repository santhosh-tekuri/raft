package raft

import (
	"bytes"
	"fmt"
	"sync"
)

type Vars interface {
	GetVote() (term uint64, vote string, err error)
	SetVote(term uint64, vote string) error
	GetConfig() (committed, latest uint64, err error)
	SetConfig(committed, latest uint64) error
}

type Log interface {
	Empty() (bool, error)

	// First returns first entry.
	// This is never called on empty log.
	First() ([]byte, error)

	// Last returns last entry.
	// This is never called on empty log.
	Last() ([]byte, error)

	Get(offset uint64) ([]byte, error)
	Append(entry []byte) error
	DeleteFirst(n uint64) error
	DeleteLast(n uint64) error
}

// todo: can we avoid panics on storage error

type Storage struct {
	vars Vars
	log  Log

	mu sync.RWMutex
	// zero for no entries. note that we never have an entry with index zero
	first, last uint64
}

func NewStorage(vars Vars, log Log) *Storage {
	return &Storage{vars: vars, log: log}
}

func (s *Storage) init() error {
	if empty, err := s.log.Empty(); err != nil || empty {
		return err
	}
	getIndex := func(get func() ([]byte, error)) (uint64, error) {
		b, err := get()
		if err != nil {
			return 0, err
		}
		entry := &entry{}
		if err = entry.decode(bytes.NewReader(b)); err != nil {
			return 0, err
		}
		return entry.index, nil
	}

	var err error
	var first, last uint64
	if first, err = getIndex(s.log.First); err != nil {
		return err
	}
	if last, err = getIndex(s.log.Last); err != nil {
		return err
	}
	s.mu.Lock()
	s.first, s.last = first, last
	s.mu.Unlock()

	return nil
}

func (s *Storage) getConfigs() (Configs, error) {
	configs := Configs{}
	committed, latest, err := s.vars.GetConfig()
	if err != nil {
		return configs, err
	}
	if committed != 0 {
		e := &entry{}
		s.getEntry(committed, e)
		if err := configs.Committed.decode(e); err != nil {
			return configs, err
		}
	}
	if latest != 0 {
		e := &entry{}
		s.getEntry(latest, e)
		if err := configs.Latest.decode(e); err != nil {
			return configs, err
		}
	}

	// handle the case, where config is stored in log but
	// crashed before saving in vars
	last, err := s.lastEntry()
	if err != nil {
		return configs, err
	}
	if last != nil && last.typ == entryConfig && last.index > latest {
		if err := configs.Latest.decode(last); err != nil {
			return configs, err
		}
	}

	return configs, nil
}

func (s *Storage) setConfigs(configs Configs) {
	if err := s.vars.SetConfig(configs.Committed.Index, configs.Latest.Index); err != nil {
		panic(err)
	}
}

func (s *Storage) count() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.first == 0 {
		return 0
	}
	return s.last - s.first + 1
}

// if empty, returns 0
func (s *Storage) getLast() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.last
}

// if empty returns nil
func (s *Storage) lastEntry() (*entry, error) {
	if s.count() == 0 {
		return nil, nil
	}
	b, err := s.log.Last()
	if err != nil {
		return nil, err
	}
	entry := &entry{}
	err = entry.decode(bytes.NewReader(b))
	return entry, err
}

// index must be valid. panic if empty
func (s *Storage) getEntry(index uint64, entry *entry) {
	if s.count() == 0 {
		panic("raft: [BUG] Storage.getEntry on empty log")
	}
	s.mu.RLock()
	offset := index - s.first
	s.mu.RUnlock()

	b, err := s.log.Get(offset)
	if err != nil {
		panic(fmt.Sprintf("raft: log.get(%d) failed: %v", index, err))
	}
	if err = entry.decode(bytes.NewReader(b)); err != nil {
		panic(fmt.Sprintf("raft: entry.decode(%d) failed: %v", index, err))
	}
}

func (s *Storage) append(entry *entry) {
	w := new(bytes.Buffer)
	if err := entry.encode(w); err != nil {
		panic(fmt.Sprintf("raft: entry.encode(%d) failed: %v", entry.index, err))
	}
	if err := s.log.Append(w.Bytes()); err != nil {
		panic(fmt.Sprintf("raft: log.append(%d): %v", entry.index, err))
	}

	s.mu.Lock()
	if s.first == 0 {
		s.first = entry.index
	}
	s.last = entry.index
	s.mu.Unlock()
}

func (s *Storage) deleteLTE(index uint64) {
	if s.count() == 0 {
		panic("raft: [BUG] Storage.deleteLTE on empty log")
	}
	n := index - s.first + 1
	if n > s.count() {
		panic(fmt.Sprintf("raft: [BUG] Storage.deleteLTE(%d) failed: not enough entries", index))
	}
	if err := s.log.DeleteFirst(n); err != nil {
		panic(fmt.Sprintf("raft: log.deleteFirst(%d) failed: %v", n, err))
	}

	s.mu.Lock()
	if index == s.last {
		s.first, s.last = 0, 0
	} else {
		s.first = index + 1
	}
	s.mu.Unlock()
}

func (s *Storage) deleteGTE(index uint64) {
	if s.count() == 0 {
		panic("raft: [BUG] Storage.deleteGTE on empty log")
	}
	n := s.last - index + 1
	if n > s.count() {
		panic(fmt.Sprintf("raft: [BUG] Storage.deleteGTE(%d) failed: not enough entries", index))
	}
	if err := s.log.DeleteLast(n); err != nil {
		panic(fmt.Sprintf("raft: log.deleteLast(%d) failed: %v", n, err))
	}

	s.mu.Lock()
	if index == s.first {
		s.first, s.last = 0, 0
	} else {
		s.last = index - 1
	}
	s.mu.Unlock()
}

func (s *Storage) bootstrap(nodes map[ID]Node) (Config, error) {
	// wipe out if log is not empty
	if s.count() > 0 {
		if err := s.log.DeleteFirst(s.count()); err != nil {
			return Config{}, err
		}
	}

	config := Config{
		Nodes: nodes,
		Index: 1,
		Term:  1,
	}
	s.append(config.encode())

	if err := s.vars.SetVote(1, ""); err != nil {
		return config, err
	}
	if err := s.vars.SetConfig(0, 1); err != nil {
		return config, err
	}
	return config, nil
}
