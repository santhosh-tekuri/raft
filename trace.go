package raft

import (
	"time"
)

type Trace struct {
	Error           func(err error)
	Starting        func(info Info)
	StateChanged    func(info Info)
	ElectionStarted func(info Info)
	ElectionAborted func(info Info, reason string)
	ConfigChanged   func(info Info)
	ConfigCommitted func(info Info)
	ConfigReverted  func(info Info)
	RoundCompleted  func(info Info, id ID, round uint64, d time.Duration, lastIndex uint64)
	Promoting       func(info Info, id ID)
	Unreachable     func(info Info, id ID, since time.Time) // todo: can we give err also
	ShuttingDown    func(info Info)

	sending  func(self, to ID, msg message)
	received func(self, from ID, msg message)
}

func DefaultTrace(info, warn func(v ...interface{})) (trace Trace) {
	trace.Error = func(err error) {
		warn(err)
	}
	trace.Starting = func(rinfo Info) {
		info("raft: starting with Config", rinfo.Configs().Latest)
	}
	trace.StateChanged = func(rinfo Info) {
		if rinfo.State() == Leader {
			info("raft: cluster leadership acquired")
		}
	}
	trace.ElectionAborted = func(rinfo Info, reason string) {
		info("raft: aborting election:", reason)
	}
	trace.ConfigChanged = func(rinfo Info) {
		if rinfo.State() == Leader {
			info("raft: config changed to", rinfo.Configs().Latest)
		}
	}
	trace.ConfigCommitted = func(rinfo Info) {
		if rinfo.State() == Leader {
			info("raft: config committed")
		}
	}
	trace.RoundCompleted = func(rinfo Info, id ID, round uint64, d time.Duration, lastIndex uint64) {
		info("raft: nonVoter", id, "completed round", round, "in", d, ", its lastIndex:", lastIndex)
	}
	trace.Promoting = func(rinfo Info, id ID) {
		info("raft: promoting node", id, "to voter")
	}
	trace.Unreachable = func(rinfo Info, id ID, since time.Time) {
		if since.IsZero() {
			info("raft: node", id, "is reachable now")
		} else {
			warn("raft: node", id, "is unreachable since", since)
		}
	}
	trace.ShuttingDown = func(rinfo Info) {
		info("raft: shutting down")
	}
	return
}

func (r *Raft) liveInfo() Info {
	return liveInfo{r: r}
}
