package raft

func (r *Raft) runLeader() {
	heartbeat := &appendEntriesRequest{
		term:     r.term,
		leaderID: r.addr,
	}

	r.notifyLastLogIndexCh()
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

		case newEntry := <-r.applyCh:
			newEntry.index = r.lastLogIndex + 1
			newEntry.term = r.term

			// append entry to local log
			r.storage.append(newEntry.entry)
			r.lastLogIndex++
			r.notifyLastLogIndexCh()

			// todo
			newEntry.respCh <- NotLeaderError{r.leaderID}

		case f := <-r.inspectCh:
			f(r)
		}
	}
}

// notify replicators about change to lastLogIndex
func (r *Raft) notifyLastLogIndexCh() {
	for _, m := range r.members {
		if m.addr != r.addr {
			select {
			case m.leaderLastIndexCh <- r.lastLogIndex:
			case <-m.leaderLastIndexCh:
				m.leaderLastIndexCh <- r.lastLogIndex
			}
		}
	}
}
