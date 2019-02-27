package raft

import (
	"fmt"
	"io"
)

// todo: add ConfigChangeInProgress, ConfigCommitted

type Trace struct {
	StateChanged    func(info Info)
	ElectionStarted func(info Info)
	ElectionAborted func(info Info, reason string)
}

func NewTrace(w io.Writer) Trace {
	return Trace{
		StateChanged: func(info Info) {
			if info.State() == Leader {
				_, _ = fmt.Fprintln(w, "[INFO] raft: cluster leadership acquired")
			}
		},
		ElectionAborted: func(info Info, reason string) {
			_, _ = fmt.Fprintf(w, "[INFO] raft: %s, aborting election\n", reason)
		},
	}
}

func (r *Raft) liveInfo() Info {
	return liveInfo{r: r}
}

func (ldr *leadership) liveInfo() Info {
	return liveInfo{r: ldr.Raft, ldr: ldr}
}

func (r *Raft) stateChanged() {
	if r.trace.StateChanged != nil {
		r.trace.StateChanged(r.liveInfo())
	}
}

func (ldr *leadership) stateChanged() {
	if ldr.trace.StateChanged != nil {
		ldr.trace.StateChanged(ldr.liveInfo())
	}
}

func (r *Raft) electionAborted(reason string) {
	if r.trace.ElectionAborted != nil {
		r.trace.ElectionAborted(r.liveInfo(), reason)
	}
}
