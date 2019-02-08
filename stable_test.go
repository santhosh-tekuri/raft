package raft

import "sync"

type inmemStable struct {
	m        sync.RWMutex
	term     uint64
	votedFor string
}

func (i *inmemStable) Get() (uint64, string, error) {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.term, i.votedFor, nil
}

func (i *inmemStable) Set(t uint64, v string) error {
	i.m.Lock()
	defer i.m.Unlock()
	i.term, i.votedFor = t, v
	return nil
}
