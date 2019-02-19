package raft

import (
	"sort"
)

func (r *Raft) runLeader() {
	assert(r.leaderID == r.addr, "r.leaderID: got %s, want %s", r.leaderID, r.addr)

	r.leaderTermStartIndex = r.lastLogIndex + 1
	assert(r.newEntries.Len() == 0, "newEntries must be empty on leader start")

	// add a blank no-op entry into log at the start of its term
	r.storeNewEntry(newEntry{
		entry: &entry{
			typ: entryNoop,
		},
	})

	recalculateMatchCh := make(chan *member, 2*len(r.members)) // room given for any new members

	// to send stop signal to replicators
	stopCh := make(chan struct{})
	defer func() {
		close(stopCh)

		if r.leaderID == r.addr {
			r.leaderID = ""
		}

		// respond to any pending user entries
		for e := r.newEntries.Front(); e != nil; e = e.Next() {
			e.Value.(newEntry).sendResponse(NotLeaderError{r.leaderID})
		}
	}()

	// to receive stale term notification from replicators
	stepDownCh := make(chan command, len(r.members))

	// start replication routine for each follower
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}

		// follower's nextIndex initialized to leader last log index + 1
		m.nextIndex = r.lastLogIndex + 1

		// send initial empty AppendEntries RPCs (heartbeat) to each follower
		req := &appendEntriesRequest{
			term:              r.term,
			leaderID:          r.addr,
			leaderCommitIndex: r.commitIndex,
			prevLogIndex:      r.lastLogIndex,
			prevLogTerm:       r.lastLogTerm,
		}
		debug(r, m.addr, ">> firstHeartbeat")
		m.retryAppendEntries(req, stopCh, stepDownCh)

		r.wg.Add(1)
		go func(m *member) {
			defer r.wg.Done()
			m.replicate(r.storage, req, stopCh, recalculateMatchCh, stepDownCh)
			debug(r, m.addr, "replicator closed")
		}(m)
	}

	for r.state == leader {
		select {
		case <-r.shutdownCh:
			return

		case cmd := <-stepDownCh:
			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			if cmd.getTerm() > r.term {
				debug(r, "leader -> follower")
				r.state = follower
				r.setTerm(cmd.getTerm())
				r.leaderID = ""
				stateChanged(r)
				return
			}

		case rpc := <-r.server.rpcCh:
			r.replyRPC(rpc)

		case m := <-recalculateMatchCh:
		loop:
			// get latest matchIndex from all notified members
			for {
				m.matchedIndex = m.getMatchIndex()
				select {
				case m = <-recalculateMatchCh:
					break
				default:
					break loop
				}
			}

			r.commitAndApplyOnMajority()

		case newEntry := <-r.applyCh:
			r.storeNewEntry(newEntry)

		case f := <-r.inspectCh:
			f(r)
		}
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (r *Raft) commitAndApplyOnMajority() {
	majorityMatchIndex := r.lastLogIndex
	if len(r.members) > 1 {
		matched := make(decrUint64Slice, len(r.members))
		for i, m := range r.members {
			if m.addr == r.addr {
				matched[i] = r.lastLogIndex
			} else {
				matched[i] = m.matchedIndex
			}
		}
		// sort in decrease order
		sort.Sort(matched)
		majorityMatchIndex = matched[r.quorumSize()-1]
	}

	if majorityMatchIndex > r.commitIndex && majorityMatchIndex >= r.leaderTermStartIndex {
		r.commitIndex = majorityMatchIndex
		r.fsmApply(r.newEntries)
		r.notifyReplicators() // we updated commit index
	}
}

func (r *Raft) storeNewEntry(newEntry newEntry) {
	newEntry.index, newEntry.term = r.lastLogIndex+1, r.term

	// append entry to local log
	if newEntry.typ == entryNoop {
		debug(r, "log.append noop", newEntry.index)
	} else {
		debug(r, "log.append cmd", newEntry.index)
	}
	r.storage.append(newEntry.entry)
	r.lastLogIndex, r.lastLogTerm = newEntry.index, newEntry.term
	r.newEntries.PushBack(newEntry)

	// we updated lastLogIndex, so notify replicators
	if len(r.members) == 1 {
		r.commitAndApplyOnMajority()
	} else {
		r.notifyReplicators()
	}
}

func (r *Raft) notifyReplicators() {
	leaderUpdate := leaderUpdate{
		lastIndex:   r.lastLogIndex,
		commitIndex: r.commitIndex,
	}
	for _, m := range r.members {
		if m.addr != r.addr {
			select {
			case m.leaderUpdateCh <- leaderUpdate:
			case <-m.leaderUpdateCh:
				m.leaderUpdateCh <- leaderUpdate
			}
		}
	}
}

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
