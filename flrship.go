package raft

type flrShip struct {
	*Raft
	electionAborted bool
}

func (f *flrShip) init() {
	f.timer.reset(f.rtime.duration(f.hbTimeout))
	f.electionAborted = false
}

func (f *flrShip) release() {}

func (f *flrShip) resetTimer() {
	if yes, _ := f.canStartElection(); yes {
		f.electionAborted = false
		f.timer.reset(f.rtime.duration(f.hbTimeout))
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
	n, ok := f.configs.Latest.Nodes[f.id]
	if !ok {
		return false, "not part of cluster"
	}
	if !n.Voter {
		return false, "not voter"
	}
	return true, ""
}
