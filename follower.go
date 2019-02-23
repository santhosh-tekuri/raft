package raft

func (r *Raft) runFollower() {
	assert(r.leader != r.addr, "%s r.leader: got %s, want !=%s", r, r.leader, r.addr)

	// todo: use single timer by resetting
	timeoutCh := afterRandomTimeout(r.heartbeatTimeout)
	for r.state == Follower {
		select {
		case <-r.shutdownCh:
			return

		case rpc := <-r.rpcCh:
			if resetElectionTimer := r.replyRPC(rpc); resetElectionTimer {
				// a server remains in follower state as long as it receives valid
				// RPCs from a leader or candidate
				timeoutCh = afterRandomTimeout(r.heartbeatTimeout)
			}

			// If timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
		case <-timeoutCh:
			debug(r, "heartbeatTimeout leader:", r.leader)
			r.leader = ""

			if can, reason := r.canStartElection(); !can {
				ElectionAborted(r, reason)
				continue // timer will be restarted on configChange
			}

			debug(r, "follower -> candidate")
			r.state = Candidate
			StateChanged(r, r.state)

		case t := <-r.TasksCh:
			before, _ := r.canStartElection()
			r.executeTask(t)
			if now, _ := r.canStartElection(); !before && now {
				// we got new config, which allows us to start election
				timeoutCh = afterRandomTimeout(r.heartbeatTimeout)
			}
		}
	}
}

func (r *Raft) canStartElection() (can bool, reason string) {
	if r.configs.isBootstrap() {
		return false, "no known peers"
	}
	if r.configs.isCommitted() && !r.configs.committed.isVoter(r.id) {
		return false, "not part of stable cluster"
	}
	return true, ""
}
