package raft

func (r *Raft) runLeader() {
	heartbeat := &appendEntriesRequest{
		term:     r.term,
		leaderID: r.addr,
	}
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}

		// follower's nextIndex initialized to leader last log index + 1
		m.nextIndex = r.lastLogIndex + 1

		stopCh := make(chan struct{})
		defer close(stopCh)
		go m.replicate(r.storage, heartbeat, r.commitIndex, stopCh)
	}

	for r.state == leader {
		select {
		case rpc := <-r.server.rpcCh:
			r.processRPC(rpc)
		}
	}
}
