package raft

import (
	"bytes"
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("not found")

type Log interface {
	First() ([]byte, error)
	Last() ([]byte, error)
	Get(offset uint64) ([]byte, error)
	Append(entry []byte) error
	DeleteFirst(n uint64) error
	DeleteLast(n uint64) error
}

type logEntries struct {
	storage Log

	// zero for no entries. note that we never have an entry with index zero
	first, last uint64
}

func (l *logEntries) lastEntry() (*entry, error) {
	b, err := l.storage.Last()
	if err != nil {
		if err == ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	entry := &entry{}
	err = entry.decode(bytes.NewReader(b))
	return entry, nil
}

func (l *logEntries) init() error {
	getIndex := func(get func() ([]byte, error)) (uint64, error) {
		b, err := get()
		if err != nil {
			if err == ErrNotFound {
				return 0, nil
			}
			return 0, err
		}
		entry := &entry{}
		if err = entry.decode(bytes.NewReader(b)); err != nil {
			return 0, err
		}
		return entry.index, nil
	}

	var err error
	if l.first, err = getIndex(l.storage.First); err != nil {
		return err
	}
	if l.last, err = getIndex(l.storage.Last); err != nil {
		return err
	}
	return nil
}

func (l *logEntries) count() uint64 {
	if l.first == 0 {
		return 0
	}
	return l.last - l.first + 1
}

func (l *logEntries) getEntry(index uint64, entry *entry) {
	offset := index - l.first
	b, err := l.storage.Get(offset)
	if err != nil {
		panic(fmt.Sprintf("failed to get entry: %v", err))
	}
	if err = entry.decode(bytes.NewReader(b)); err != nil {
		panic(fmt.Sprintf("failed to decode stored entry: %v", err))
	}
}

func (l *logEntries) append(entry *entry) {
	w := new(bytes.Buffer)
	if err := entry.encode(w); err != nil {
		panic(fmt.Sprintf("failed to encode entry: %v", err))
	}
	if err := l.storage.Append(w.Bytes()); err != nil {
		panic(fmt.Sprintf("failed to append entry: %v", err))
	}
	if l.first == 0 {
		l.first = entry.index
	}
	l.last = entry.index
}

func (l *logEntries) deleteLTE(index uint64) {
	if l.count() == 0 {
		panic("[BUG] deleteLTE on empty log")
	}
	n := index - l.first + 1
	if n > l.count() {
		panic("[BUG] deleteLTE: not enough entries")
	}
	if err := l.storage.DeleteFirst(n); err != nil {
		panic(fmt.Sprintf("deleteFirst failed: %v", err))
	}
	if index == l.last {
		l.first, l.last = 0, 0
	} else {
		l.first = index + 1
	}
}

func (l *logEntries) deleteGTE(index uint64) {
	if l.count() == 0 {
		panic("[BUG] deleteGTE on empty log")
	}
	n := l.last - index + 1
	if n > l.count() {
		panic("[BUG] deleteGTE: not enough entries")
	}
	if err := l.storage.DeleteLast(n); err != nil {
		panic(fmt.Sprintf("deleteLast failed: %v", err))
	}
	if index == l.first {
		l.first, l.last = 0, 0
	} else {
		l.last = index - 1
	}
}
