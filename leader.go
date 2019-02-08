package raft

func (r *Raft) runLeader() {
	heartbeat := &appendEntriesRequest{
		term:     r.term,
		leaderID: r.addr,
	}

	r.notifyLastLogIndexCh()
	r.notifyCommitIndexCh()

	// start replication routine for each follower
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}

		// follower's nextIndex initialized to leader last log index + 1
		m.nextIndex = r.lastLogIndex + 1

		stopCh := make(chan struct{})
		defer close(stopCh)
		go m.replicate(r.storage, heartbeat, stopCh)
	}

	// add a blank no-op entry into log at the start of its term
	r.processNewEntry(newEntry{
		entry: &entry{
			typ: entryNoop,
		},
	})

	for r.state == leader {
		select {
		case rpc := <-r.server.rpcCh:
			r.processRPC(rpc)

		case newEntry := <-r.applyCh:
			r.processNewEntry(newEntry)
		case f := <-r.inspectCh:
			f(r)
		}
	}
}

func (r *Raft) processNewEntry(newEntry newEntry) {
	newEntry.index = r.lastLogIndex + 1
	newEntry.term = r.term

	// append entry to local log
	r.storage.append(newEntry.entry)
	r.lastLogIndex++
	r.notifyLastLogIndexCh()

	// todo
	newEntry.sendReesponse(NotLeaderError{r.leaderID})
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

// notify replicators about change to commitIndex
func (r *Raft) notifyCommitIndexCh() {
	for _, m := range r.members {
		if m.addr != r.addr {
			select {
			case m.leaderCommitIndexCh <- r.commitIndex:
			case <-m.leaderCommitIndexCh:
				m.leaderCommitIndexCh <- r.commitIndex
			}
		}
	}
}
