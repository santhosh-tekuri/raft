package raft

type follower struct {
	*Raft
	electionAborted bool
}

func (f *follower) init() {
	f.timer.reset(f.rtime.duration(f.hbTimeout))
	f.electionAborted = false
}

func (f *follower) release() {}

func (f *follower) resetTimer() {
	if yes, _ := f.canStartElection(); yes {
		f.electionAborted = false
		f.timer.reset(f.rtime.duration(f.hbTimeout))
	}
}

func (f *follower) onTimeout() {
	debug(f, "heartbeatTimeout leader:", f.leader)
	f.setLeader(0)
	if can, reason := f.canStartElection(); !can {
		debug(f, "electionAborted", reason)
		f.electionAborted = true
		if f.trace.ElectionAborted != nil {
			f.trace.ElectionAborted(f.liveInfo(), reason)
		}
		return
	}
	f.setState(Candidate)
}

func (f *follower) canStartElection() (can bool, reason string) {
	if f.configs.IsBootstrap() {
		return false, "no known peers"
	}
	n, ok := f.configs.Latest.Nodes[f.nid]
	if !ok {
		return false, "not part of cluster"
	}
	if !n.Voter {
		return false, "not voter"
	}
	return true, ""
}
