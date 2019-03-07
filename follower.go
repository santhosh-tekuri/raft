package raft

import "time"

type flrShip struct {
	*Raft
	timeoutCh       <-chan time.Time
	electionAborted bool
}

func (f *flrShip) init() {
	f.timeoutCh = afterRandomTimeout(f.hbTimeout)
	f.electionAborted = false
}

func (f *flrShip) release() {
	f.timeoutCh = nil
}

func (f *flrShip) resetTimer() {
	if yes, _ := f.canStartElection(); yes {
		f.electionAborted = false
		f.timeoutCh = afterRandomTimeout(f.hbTimeout)
	}
}

func (f *flrShip) onTimeout() {
	debug(f, "heartbeatTimeout leader:", f.leader)
	f.leader = ""

	if can, reason := f.canStartElection(); !can {
		debug(f, "electionAborted", reason)
		f.electionAborted = true
		if f.trace.ElectionAborted != nil {
			f.trace.ElectionAborted(f.liveInfo(), reason)
		}
		return
	}

	debug(f, "follower -> candidate")
	f.state = Candidate
}

func (f *flrShip) canStartElection() (can bool, reason string) {
	if f.configs.IsBootstrap() {
		return false, "no known peers"
	}
	if f.configs.IsCommitted() {
		n, ok := f.configs.Latest.Nodes[f.id]
		if !ok {
			return false, "not part of cluster"
		}
		if !n.Voter {
			return false, "not voter"
		}
	}
	return true, ""
}
