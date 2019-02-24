package raft

import (
	"time"
)

type member struct {
	id NodeID

	// owned exclusively by raft main goroutine
	// used to recalculateMatch
	matchIndex uint64

	// from what time the replication unable to reach this member
	// zero value means it is reachable
	noContact time.Time

	str string
}

func (m *member) String() string {
	return m.str
}

// did we have success full contact after time t
func (m *member) contactedAfter(t time.Time) bool {
	return m.noContact.IsZero() || m.noContact.After(t)
}
