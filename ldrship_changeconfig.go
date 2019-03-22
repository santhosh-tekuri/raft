package raft

import (
	"fmt"
	"time"
)

// called on ldrShip.init and configChange
func (l *ldrShip) onActionChange() {
	for id, f := range l.flrs {
		n := l.configs.Latest.Nodes[id]
		if !n.promote() {
			f.status.round = nil
		} else if f.status.round == nil {
			// start first round
			f.status.round = new(Round)
			f.status.round.begin(f.ldrLastIndex)
			debug(l, id, "started:", f.status.round)
		}
	}
	l.checkActions()
}

// called on lastLogIndex update
func (l *ldrShip) beginFinishedRounds() {
	for id, f := range l.flrs {
		r := f.status.round
		if r != nil && r.finished() {
			r.begin(f.ldrLastIndex)
			debug(l, id, "started:", r)
		}
	}
}

// checkActions finishes any postponed promotions
//
// called on configCommit and transferLdr.timout
func (l *ldrShip) checkActions() {
	for _, f := range l.flrs {
		l.checkActionStatus(&f.status)
	}
}

// checks whether round is completed, if so
// promotes if threshold is satisfied.
//
// called when f.matchIndex is updated or by checkActions
func (l *ldrShip) checkActionStatus(status *flrStatus) {
	if status.round != nil {
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
		hasNewEntries := l.lastLogIndex > status.matchIndex
		if hasNewEntries && r.Duration() > l.promoteThreshold {
			debug(l, "best of luck for next round")
			r.begin(l.lastLogIndex)
			debug(l, status.id, "started:", r)
			return
		}
	}

	if !l.canChangeConfig() {
		n := l.configs.Latest.Nodes[status.id]
		if n.Action != None {
			debug(l, status.id, "cannot", n.Action, "now")
		}
		return
	}

	n := l.configs.Latest.Nodes[status.id]
	if n.promote() {
		debug(l, "promoting", n.ID)
		config := l.configs.Latest.clone()
		n.Voter, n.Action = true, None
		config.Nodes[n.ID] = n
		if l.trace.ConfigActionStarted != nil {
			l.trace.ConfigActionStarted(l.liveInfo(), n.ID, Promote)
		}
		l.doChangeConfig(nil, config)
	} else if n.remove() {
		if status.matchIndex >= l.configs.Latest.Index {
			debug(l, "removing", n.ID)
			config := l.configs.Latest.clone()
			delete(config.Nodes, n.ID)
			if l.trace.ConfigActionStarted != nil {
				l.trace.ConfigActionStarted(l.liveInfo(), n.ID, Remove)
			}
			l.doChangeConfig(nil, config)
		}
	} else if n.demote() {
		debug(l, "demoting", n.ID)
		config := l.configs.Latest.clone()
		n.Voter = false
		if n.Action == Demote {
			n.Action = None
		}
		config.Nodes[n.ID] = n
		if l.trace.ConfigActionStarted != nil {
			l.trace.ConfigActionStarted(l.liveInfo(), n.ID, Demote)
		}
		l.doChangeConfig(nil, config)
	}
}

func (l *ldrShip) canChangeConfig() bool {
	return l.configs.IsCommitted() && !l.transfer.inProgress()
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
