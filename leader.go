package raft

import (
	"sort"
	"time"
)

func (r *Raft) runLeader() {
	assert(r.leaderID == r.addr, "%s r.leaderID: got %s, want %s", r, r.leaderID, r.addr)

	r.leaderTermStartIndex = r.lastLogIndex + 1
	assert(r.newEntries.Len() == 0, "%s newEntries must be empty on leader start", r)

	// add a blank no-op entry into log at the start of its term
	r.storeNewEntry(newEntry{
		entry: &entry{
			typ: entryNoop,
		},
	})

	// to receive new term notifications from replicators
	newTermCh := make(chan uint64, len(r.members))

	// to receive matchIndex updates from replicators
	matchUpdatedCh := make(chan *member, len(r.members))

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

	// start replication routine for each follower
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}

		// follower's nextIndex initialized to leader last log index + 1
		m.nextIndex = r.lastLogIndex + 1
		m.matchedIndex = m.getMatchIndex()
		m.noContactMu.Lock() //todo: should we take lock. just ensure replicators are closed on runLeader return
		m.noContact = time.Time{}
		m.noContactMu.Unlock()
		m.ldr = r.String()

		// send initial empty AppendEntries RPCs (heartbeat) to each follower
		req := &appendEntriesRequest{
			term:              r.term,
			leaderID:          r.addr,
			leaderCommitIndex: r.commitIndex,
			prevLogIndex:      r.lastLogIndex,
			prevLogTerm:       r.lastLogTerm,
		}
		debug(r, m.addr, ">> firstHeartbeat")
		m.retryAppendEntries(req, r.shutdownCh, newTermCh)

		r.wg.Add(1)
		go func(m *member) {
			defer r.wg.Done()
			m.replicate(req, stopCh, matchUpdatedCh, newTermCh)
			debug(m.ldr, m.addr, "replicator closed")
		}(m)
	}

	leaseTimer := time.NewTicker(r.leaderLeaseTimeout)
	defer leaseTimer.Stop()

	for r.state == leader {
		select {
		case <-r.shutdownCh:
			return

		case newTerm := <-newTermCh:
			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			debug(r, "leader -> follower")
			r.state = follower
			r.setTerm(newTerm)
			r.leaderID = ""
			stateChanged(r)
			return

		case rpc := <-r.server.rpcCh:
			r.replyRPC(rpc)

		case m := <-matchUpdatedCh:
		loop:
			// get latest matchIndex from all notified members
			for {
				m.matchedIndex = m.getMatchIndex()
				select {
				case m = <-matchUpdatedCh:
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

		case <-leaseTimer.C:
			if !r.isQuorumReachable() {
				r.state = follower
				r.leaderID = ""
				stateChanged(r)
			}
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

func (r *Raft) isQuorumReachable() bool {
	reachable := 1 // including self
	now := time.Now()
	for _, m := range r.members {
		if m.addr != r.addr {
			m.noContactMu.RLock()
			noContact := m.noContact
			m.noContactMu.RUnlock()
			if noContact.IsZero() || now.Sub(noContact) < r.leaderLeaseTimeout {
				reachable++
			}
		}
	}
	if reachable < r.quorumSize() {
		debug(r, "quorumUnreachable: ", reachable, "<", r.quorumSize())
	}
	return reachable >= r.quorumSize()
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
