package inmem

import (
	"errors"
	"sync"
)

var (
	ErrNotFound   = errors.New("not found")
	ErrOutOfRange = errors.New("out of range")
)

type Storage struct {
	muStable      sync.RWMutex
	term          uint64
	vote          string
	confCommitted uint64
	confLatest    uint64

	muLog sync.RWMutex
	list  [][]byte
}

func (s *Storage) GetVote() (term uint64, vote string, err error) {
	s.muStable.RLock()
	defer s.muStable.RUnlock()
	return s.term, s.vote, nil
}

func (s *Storage) SetVote(term uint64, vote string) error {
	s.muStable.Lock()
	defer s.muStable.Unlock()
	s.term, s.vote = term, vote
	return nil
}

func (s *Storage) GetConfig() (committed, latest uint64, err error) {
	s.muStable.RLock()
	defer s.muStable.RUnlock()
	return s.confCommitted, s.confLatest, nil
}

func (s *Storage) SetConfig(committed, latest uint64) error {
	s.muStable.Lock()
	defer s.muStable.Unlock()
	s.confCommitted, s.confLatest = committed, latest
	return nil
}

func (s *Storage) Empty() (bool, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	return len(s.list) == 0, nil
}

func (s *Storage) First() ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if len(s.list) == 0 {
		return nil, ErrNotFound
	}
	return s.list[0], nil
}

func (s *Storage) Last() ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if len(s.list) == 0 {
		return nil, ErrNotFound
	}
	return s.list[len(s.list)-1], nil
}

func (s *Storage) Get(offset uint64) ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if offset >= uint64(len(s.list)) {
		return nil, ErrNotFound
	}
	return s.list[offset], nil
}

func (s *Storage) Append(entry []byte) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	s.list = append(s.list, entry)
	return nil
}

func (s *Storage) DeleteFirst(n uint64) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	if n > uint64(len(s.list)) {
		return ErrOutOfRange
	}
	s.list = s.list[n:]
	return nil
}

func (s *Storage) DeleteLast(n uint64) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	if n > uint64(len(s.list)) {
		return ErrOutOfRange
	}
	s.list = s.list[:len(s.list)-int(n)]
	return nil
}
