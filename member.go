package raft

import (
	"sync"
	"time"
)

type leaderUpdate struct {
	lastIndex, commitIndex uint64
}

type member struct {
	addr     string
	connPool *connPool

	// owned exclusively by raft main goroutine
	// used to recalculateMatch
	matchIndex uint64

	// from what time the replication unable to reach this member
	// zero value means it is reachable
	noContactMu sync.RWMutex
	noContact   time.Time
}

func (m *member) contactSucceeded(b bool) {
	m.noContactMu.Lock()
	if b {
		m.noContact = time.Time{} // zeroing
	} else if m.noContact.IsZero() {
		m.noContact = time.Now()
	}
	m.noContactMu.Unlock()
}

// did we have success full contact after time t
func (m *member) contactedAfter(t time.Time) bool {
	m.noContactMu.RLock()
	noContact := m.noContact
	m.noContactMu.RUnlock()
	return noContact.IsZero() || noContact.After(t)
}
