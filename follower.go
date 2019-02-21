package raft

func (r *Raft) runFollower() {
	assert(r.leaderID != r.addr, "%s r.leaderID: got %s, want !=%s", r, r.leaderID, r.addr)

	timeoutCh := afterRandomTimeout(r.heartbeatTimeout)
	for r.state == follower {
		select {
		case <-r.shutdownCh:
			return

		case rpc := <-r.rpcCh:
			if valid := r.replyRPC(rpc); valid {
				// a server remains in follower state as long as it receives valid
				// RPCs from a leader or candidate
				timeoutCh = afterRandomTimeout(r.heartbeatTimeout)
			}

			// If timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
		case <-timeoutCh:
			if !r.config.canStartElection(r.addr) {
				// todo: abstract log
				if len(r.config.members()) == 0 {
					ElectionAborted(r, "no known peers")
				} else if _, ok := r.config.(*stableConfig); ok {
					ElectionAborted(r, "not part of cluster")
				} else {
					// jointConfig is in use, and i am not part of Cold, thus can not be
					// elected leader
				}
				r.leaderID = ""
				// timer will be restarted, when we receive rpc request
				// because config will not be changed without receiving appendEntries
				// with configChange type
				continue
			}
			debug(r, "heartbeatTimeout follower -> candidate")
			r.state = candidate
			r.leaderID = ""
			StateChanged(r, byte(r.state))

		case t := <-r.TasksCh:
			before := r.config.canStartElection(r.addr)
			t.execute(r)
			if !before && r.config.canStartElection(r.addr) {
				// we go config, which allows us to start election
				timeoutCh = afterRandomTimeout(r.heartbeatTimeout)
			}
		}
	}
}
