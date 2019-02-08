package raft

func (r *Raft) runFollower() {
	r.electionTimer = randomTimer(r.heartbeatTimeout)
	for {
		select {
		case rpc := <-r.server.calls:
			r.processRPC(rpc)

			// restart timer
			r.electionTimer = randomTimer(r.heartbeatTimeout)

			// If election timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
		case <-r.electionTimer.C:
			// heartbeat failed. transition to candidate
			debug(r, "no contact within electionTimeout")
			r.state = candidate
			return
		}
	}
}
