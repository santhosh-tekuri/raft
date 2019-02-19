package raft

func (r *Raft) runFollower() {
	assert(r.leaderID != r.addr, "r.leaderID: got %s, want !=%s", r.leaderID, r.addr)

	timeoutCh := afterRandomTimeout(r.heartbeatTimeout)
	for r.state == follower {
		select {
		case <-r.shutdownCh:
			return

		case rpc := <-r.server.rpcCh:
			if valid := r.replyRPC(rpc); valid {
				// a server remains in follower state as long as it receives valid
				// RPCs from a leader or candidate
				timeoutCh = afterRandomTimeout(r.heartbeatTimeout)
			}

			// If timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
		case <-timeoutCh:
			debug(r, "heartbeatTimeout follower -> candidate")
			r.state = candidate
			stateChanged(r)

		case newEntry := <-r.applyCh:
			newEntry.sendResponse(NotLeaderError{r.leaderID})

		case f := <-r.inspectCh:
			f(r)
		}
	}
}
