package raft

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

type Stable interface {
	GetVars() (term uint64, votedFor string, err error)
	SetVars(term uint64, votedFor string) error
	GetConfig() (committed, latest uint64, err error)
	SetConfig(committed, latest uint64) error
}

type Log interface {
	Empty() (bool, error)
	First() ([]byte, error)
	Last() ([]byte, error)
	Get(offset uint64) ([]byte, error)
	Append(entry []byte) error
	DeleteFirst(n uint64) error
	DeleteLast(n uint64) error
}

type storage struct {
	Stable // todo: cache values
	log    Log

	mu sync.RWMutex
	// zero for no entries. note that we never have an entry with index zero
	first, last uint64
}

func (s *storage) getLast() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.last
}

func (s *storage) lastEntry() (*entry, error) {
	if s.count() == 0 {
		return nil, nil
	}
	b, err := s.log.Last()
	if err != nil {
		return nil, err
	}
	entry := &entry{}
	err = entry.decode(bytes.NewReader(b))
	return entry, nil
}

func (s *storage) init() error {
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

func (s *storage) count() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.first == 0 {
		return 0
	}
	return s.last - s.first + 1
}

// todo: can we avoid panics

func (s *storage) getEntry(index uint64, entry *entry) {
	s.mu.RLock()
	offset := index - s.first
	s.mu.RUnlock()

	b, err := s.log.Get(offset)
	if err != nil {
		panic(fmt.Sprintf("failed to get entry: %v", err))
	}
	if err = entry.decode(bytes.NewReader(b)); err != nil {
		panic(fmt.Sprintf("failed to decode stored entry: %v", err))
	}
}

func (s *storage) append(entry *entry) {
	w := new(bytes.Buffer)
	if err := entry.encode(w); err != nil {
		panic(fmt.Sprintf("failed to encode entry: %v", err))
	}
	if err := s.log.Append(w.Bytes()); err != nil {
		panic(fmt.Sprintf("failed to append entry: %v", err))
	}

	s.mu.Lock()
	if s.first == 0 {
		s.first = entry.index
	}
	s.last = entry.index
	s.mu.Unlock()
}

func (s *storage) deleteLTE(index uint64) {
	if s.count() == 0 {
		panic("[BUG] deleteLTE on empty log")
	}
	n := index - s.first + 1
	if n > s.count() {
		panic("[BUG] deleteLTE: not enough entries")
	}
	if err := s.log.DeleteFirst(n); err != nil {
		panic(fmt.Sprintf("deleteFirst failed: %v", err))
	}

	s.mu.Lock()
	if index == s.last {
		s.first, s.last = 0, 0
	} else {
		s.first = index + 1
	}
	s.mu.Unlock()
}

func (s *storage) deleteGTE(index uint64) {
	if s.count() == 0 {
		panic("[BUG] deleteGTE on empty log")
	}
	n := s.last - index + 1
	if n > s.count() {
		panic("[BUG] deleteGTE: not enough entries")
	}
	if err := s.log.DeleteLast(n); err != nil {
		panic(fmt.Sprintf("deleteLast failed: %v", err))
	}

	s.mu.Lock()
	if index == s.first {
		s.first, s.last = 0, 0
	} else {
		s.last = index - 1
	}
	s.mu.Unlock()
}

// fills entries in range [nextIndex, lastIndex] inclusive into given request
func (s *storage) fillEntries(req *appendEntriesRequest, nextIndex, lastIndex uint64) {
	if nextIndex == 1 {
		req.prevLogIndex, req.prevLogTerm = 0, 0
	} else {
		prevEntry := &entry{}
		s.getEntry(nextIndex-1, prevEntry)
		req.prevLogIndex, req.prevLogTerm = prevEntry.index, prevEntry.term
	}

	if lastIndex-nextIndex+1 == 0 {
		req.entries = nil
	} else {
		req.entries = make([]*entry, lastIndex-nextIndex+1)
		for i := range req.entries {
			req.entries[i] = &entry{}
			s.getEntry(nextIndex+uint64(i), req.entries[i])
		}
	}
}

func (s *storage) bootstrap(addrs []string, dialFn dialFn, timeout time.Duration) (config, error) {
	members := make(map[string]*member)
	for _, addr := range addrs {
		members[addr] = &member{
			dialFn:  dialFn,
			addr:    addr,
			timeout: timeout,
		}
	}
	config := &stableConfig{members}
	bytes, err := encodeConfig(config)
	if err != nil {
		return nil, err
	}
	e := &entry{
		typ:   entryConfig,
		index: 1,
		term:  1,
		data:  bytes,
	}

	s.append(e)
	if err := s.SetVars(1, ""); err != nil {
		return nil, err
	}
	if err := s.SetConfig(0, 1); err != nil {
		return nil, err
	}
	return config, nil
}
