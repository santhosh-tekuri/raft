package raft

func (r *Raft) runFollower() {
	r.electionTimer = randomTimer(r.heartbeatTimeout)
	for {
		select {
		case rpc := <-r.server.rpcCh:
			r.processRPC(rpc)

			// restart timer
			r.electionTimer = randomTimer(r.heartbeatTimeout)

			// If election timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
		case <-r.electionTimer.C:
			// heartbeat failed. transition to candidate
			debug(r, "electionTimeout follower -> candidate")
			r.state = candidate
			return
		case newEntry := <-r.applyCh:
			newEntry.respCh <- NotLeaderError{r.leaderID}
		}
	}
}
