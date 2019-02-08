package raft

import "sync"

type inmemLog struct {
	m    sync.RWMutex
	list [][]byte
}

func (i *inmemLog) First() ([]byte, error) {
	i.m.RLock()
	defer i.m.RUnlock()
	if len(i.list) == 0 {
		return nil, ErrNotFound
	}
	return i.list[0], nil
}

func (i *inmemLog) Last() ([]byte, error) {
	i.m.RLock()
	defer i.m.RUnlock()
	if len(i.list) == 0 {
		return nil, ErrNotFound
	}
	return i.list[len(i.list)-1], nil
}

func (i *inmemLog) Get(offset uint64) ([]byte, error) {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.list[offset], nil
}

func (i *inmemLog) Append(entry []byte) error {
	i.m.Lock()
	defer i.m.Unlock()
	i.list = append(i.list, entry)
	return nil
}

func (i *inmemLog) DeleteFirst(n uint64) error {
	i.m.Lock()
	defer i.m.Unlock()
	i.list = i.list[n:]
	return nil
}

func (i *inmemLog) DeleteLast(n uint64) error {
	i.m.Lock()
	defer i.m.Unlock()
	i.list = i.list[:len(i.list)-1-int(n)]
	return nil
}
