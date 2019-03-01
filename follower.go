package raft

func (r *Raft) runFollower() {
	assert(r.leader != r.id, "%s r.leader: got %s, want !=%s", r, r.leader, r.id)

	// todo: use single timer by resetting
	timeoutCh := afterRandomTimeout(r.hbTimeout)
	for r.state == Follower {
		select {
		case <-r.shutdownCh:
			return

		case rpc := <-r.server.rpcCh:
			if validReq := r.replyRPC(rpc); validReq {
				if yes, _ := r.canStartElection(); yes {
					// a server remains in follower state as long as it receives valid
					// RPCs from a leader or candidate
					timeoutCh = afterRandomTimeout(r.hbTimeout)
				}
			}

			// If timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
		case <-timeoutCh:
			debug(r, "heartbeatTimeout leader:", r.leader)
			r.leader = ""

			if can, reason := r.canStartElection(); !can {
				if r.trace.ElectionAborted != nil {
					r.trace.ElectionAborted(r.liveInfo(), reason)
				}
				continue // timer will be restarted on configChange
			}

			debug(r, "follower -> candidate")
			r.state = Candidate
			r.stateChanged()

		case ne := <-r.newEntryCh:
			ne.reply(NotLeaderError{r.leaderAddr(), false})

		case t := <-r.taskCh:
			before, _ := r.canStartElection()
			r.executeTask(t)
			if now, _ := r.canStartElection(); !before && now {
				// we got new config, which allows us to start election
				timeoutCh = afterRandomTimeout(r.hbTimeout)
			}
		}
	}
}

func (r *Raft) canStartElection() (can bool, reason string) {
	if r.configs.IsBootstrap() {
		return false, "no known peers"
	}
	if r.configs.IsCommitted() {
		n, ok := r.configs.Latest.Nodes[r.id]
		if !ok {
			return false, "not part of cluster"
		}
		if !n.Voter {
			return false, "not voter"
		}
	}
	return true, ""
}
