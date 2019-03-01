package raft

import (
	"fmt"
	"io"
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

func NewTraceWriter(w io.Writer) Trace {
	return Trace{
		Starting: func(info Info) {
			_, _ = fmt.Fprintf(w, "[INFO] raft: starting with Config %s\n", info.Configs().Latest)
		},
		LookupIDFailed: func(id ID, err error, fallbackAddr string) {
			_, _ = fmt.Fprintf(w, "[INFO] raft: lookupID(%s) failed, using fallback addr %s: %v\n", id, fallbackAddr, err)
		},
		StateChanged: func(info Info) {
			if info.State() == Leader {
				_, _ = fmt.Fprintln(w, "[INFO] raft: cluster leadership acquired")
			}
		},
		ElectionAborted: func(info Info, reason string) {
			_, _ = fmt.Fprintf(w, "[INFO] raft: %s, aborting election\n", reason)
		},
		ConfigChanged: func(info Info) {
			_, _ = fmt.Fprintf(w, "[INFO] raft: config changed to %s\n", info.Configs().Latest)
		},
		ConfigCommitted: func(info Info) {
			_, _ = fmt.Fprintln(w, "[INFO] raft: config committed")
		},
		ConfigReverted: func(info Info) {
			_, _ = fmt.Fprintf(w, "[INFO] raft: config reverted to %s\n", info.Configs().Latest)
		},
		Unreachable: func(info Info, id ID, since time.Time) {
			if since.IsZero() {
				_, _ = fmt.Fprintf(w, "[INFO] raft: node %s is reachable\n", id)
			} else {
				_, _ = fmt.Fprintf(w, "[INFO] raft: node %s is unreachable since %s\n", id, since)
			}
		},
		ShuttingDown: func(info Info) {
			_, _ = fmt.Fprintln(w, "[INFO] raft: shutting down")
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
