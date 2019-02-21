package raft

import "log"

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
					log.Println("[INFO] raft: no known peers, aborting election.")
				} else if _, ok := r.config.(*stableConfig); ok {
					log.Println("[INFO] raft: not part of stable configuration, aborting election.")
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
			stateChanged(r)

		case ne := <-r.ApplyCh:
			ne.sendResponse(NotLeaderError{r.leaderID})

		case f := <-r.inspectCh:
			f(r)
		}
	}
}
