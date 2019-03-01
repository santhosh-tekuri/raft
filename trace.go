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

func DefaultTrace(infof, warnf func(format string, v ...interface{})) Trace {
	return Trace{
		Starting: func(info Info) {
			infof("raft: starting with Config %s\n", info.Configs().Latest)
		},
		LookupIDFailed: func(id ID, err error, fallbackAddr string) {
			warnf("raft: lookupID(%s) failed, using fallback addr %s: %v\n", id, fallbackAddr, err)
		},
		StateChanged: func(info Info) {
			if info.State() == Leader {
				infof("raft: cluster leadership acquired\n")
			}
		},
		ElectionAborted: func(info Info, reason string) {
			infof("raft: %s, aborting election\n", reason)
		},
		ConfigChanged: func(info Info) {
			infof("raft: config changed to %s\n", info.Configs().Latest)
		},
		ConfigCommitted: func(info Info) {
			infof("raft: config committed\n")
		},
		ConfigReverted: func(info Info) {
			infof("raft: config reverted to %s\n", info.Configs().Latest)
		},
		Unreachable: func(info Info, id ID, since time.Time) {
			if since.IsZero() {
				infof("raft: node %s is reachable\n", id)
			} else {
				warnf("raft: node %s is unreachable since %s\n", id, since)
			}
		},
		ShuttingDown: func(info Info) {
			infof("raft: shutting down\n")
		},
	}
}

func (r *Raft) liveInfo() Info {
	return liveInfo{r: r, ldr: r.ldr}
}

func (r *Raft) stateChanged() {
	if r.trace.StateChanged != nil {
		r.trace.StateChanged(r.liveInfo())
	}
}
