package raft

import (
	"time"
)

type Trace struct {
	Starting        func(info Info)
	LookupIDFailed  func(id ID, err error, fallbackAddr string)
	StateChanged    func(info Info)
	ElectionStarted func(info Info)
	ElectionAborted func(info Info, reason string)
	ConfigChanged   func(info Info)
	ConfigCommitted func(info Info)
	ConfigReverted  func(info Info)
	Unreachable     func(info Info, id ID, since time.Time) // todo: can we give err also
	ShuttingDown    func(info Info)
}

func DefaultTrace(info, warn func(v ...interface{})) (trace Trace) {
	trace.Starting = func(rinfo Info) {
		info("raft: starting with Config", rinfo.Configs().Latest)
	}
	trace.LookupIDFailed = func(id ID, err error, fallbackAddr string) {
		warn("raft.lookupID: using fallback addr", fallbackAddr, "for", id, ":", err)
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
	return liveInfo{r: r, ldr: r.ldr}
}

func (r *Raft) stateChanged() {
	if r.trace.StateChanged != nil {
		r.trace.StateChanged(r.liveInfo())
	}
}
