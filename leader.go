package raft

func (r *Raft) runLeader() {
	// send initial empty AppendEntries RPCs
	// (heartbeat) to each server; repeat during idle periods to
	// prevent election timeouts
	req := &appendEntriesRequest{
		term:     r.term,
		leaderID: r.addr,
	}
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}
		stopCh := make(chan struct{})
		defer close(stopCh)
		go m.sendHeartbeats(req, stopCh)
	}

	for r.state == leader {
		select {
		case rpc := <-r.server.calls:
			r.processRPC(rpc)
		}
	}
}
