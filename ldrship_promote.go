package raft

import (
	"fmt"
	"time"
)

// called on ldrShip.init and configChange
func (l *ldrShip) beginCancelRounds() {
	for id, f := range l.flrs {
		n := l.configs.Latest.Nodes[id]
		if !n.promote() {
			f.status.round = nil
		} else if f.status.round == nil {
			// start first round
			f.status.round = new(Round)
			f.status.round.begin(f.ldrLastIndex)
			debug(f, id, "started:", f.status.round)
		}
	}
}

// called on lastLogIndex update
func (l *ldrShip) beginFinishedRounds() {
	for id, f := range l.flrs {
		r := f.status.round
		if r != nil && r.finished() {
			r.begin(f.ldrLastIndex)
			debug(f, id, "started:", r)
		}
	}
}

// promotePending finishes any postponed promotions
//
// called on configCommit and transferLdr.timout
func (l *ldrShip) promotePending() {
	for _, f := range l.flrs {
		r := f.status.round
		if r != nil && r.finished() {
			l.checkPromotion(&f.status)
		}
	}
}

// checks whether round is completed, if so
// promotes if threshold is satisfied.
//
// called when f.matchIndex is updated or by promotePending
func (l *ldrShip) checkPromotion(status *flrStatus) {
	r := status.round
	if !r.finished() && status.matchIndex >= r.LastIndex {
		r.finish()
		debug(l, status.id, "completed:", r)
		if l.trace.RoundCompleted != nil {
			l.trace.RoundCompleted(l.liveInfo(), status.id, *r)
		}
	}
	if !r.finished() {
		return
	}
	n, ok := l.configs.Latest.Nodes[status.id]
	if !ok || !n.promote() {
		debug(l, "this node should not be promoted")
		status.round = nil
		return
	}
	hasNewEntries := l.lastLogIndex > status.matchIndex
	if hasNewEntries && r.Duration() > l.promoteThreshold {
		debug(l, "best of luck for next round")
		r.begin(l.lastLogIndex)
		debug(l, status.id, "started:", r)
		return
	}

	// can promote ?
	if l.transfer.inProgress() {
		debug(l, "cannot promote: transferLeadership in progress")
		return
	}
	if !l.configs.IsCommitted() {
		debug(l, "cannot promote: config not committed")
		return
	}

	// promoting flr
	debug(l, "promoting", n.ID)
	config := l.configs.Latest.clone()
	n.Voter, n.Promote = true, false
	config.Nodes[n.ID] = n
	if l.trace.Promoting != nil {
		l.trace.Promoting(l.liveInfo(), n.ID, r.Ordinal)
	}
	l.doChangeConfig(nil, config)
}

// Round ------------------------------------------------

type Round struct {
	Ordinal   uint64
	Start     time.Time
	End       time.Time
	LastIndex uint64
}

func (r *Round) begin(lastIndex uint64) {
	r.Ordinal, r.Start, r.LastIndex = r.Ordinal+1, time.Now(), lastIndex
}
func (r *Round) finish()                { r.End = time.Now() }
func (r *Round) finished() bool         { return !r.End.IsZero() }
func (r Round) Duration() time.Duration { return r.End.Sub(r.Start) }

func (r Round) String() string {
	if r.finished() {
		return fmt.Sprintf("round{#%d %s lastIndex: %d}", r.Ordinal, r.Duration(), r.LastIndex)
	}
	return fmt.Sprintf("round{#%d lastIndex: %d}", r.Ordinal, r.LastIndex)
}
