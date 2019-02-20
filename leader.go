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
	r.storeNewEntry(NewEntry{
		entry: &entry{
			typ: entryNoop,
		},
	}, nil)

	// to receive new term notifications from replicators
	newTermCh := make(chan uint64, len(r.members))

	// to receive matchIndex updates from replicators
	matchUpdatedCh := make(chan *replicator, len(r.members))

	// to send stop signal to replicators
	stopReplsCh := make(chan struct{})

	defer func() {
		close(stopReplsCh)

		if r.leaderID == r.addr {
			r.leaderID = ""
		}

		// respond to any pending user entries
		for e := r.newEntries.Front(); e != nil; e = e.Next() {
			e.Value.(NewEntry).sendResponse(NotLeaderError{r.leaderID})
		}
	}()

	// start replication routine for each follower
	repls := make(map[string]*replicator) // todo: move repls as field in Raft
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}

		repl := &replicator{
			member:           m,
			heartbeatTimeout: r.heartbeatTimeout,
			storage:          r.storage,
			nextIndex:        r.lastLogIndex + 1, // nextIndex initialized to leader last log index + 1
			matchIndex:       0,                  // matchIndex initialized to zero
			matchedIndex:     0,
			stopCh:           stopReplsCh,
			matchUpdatedCh:   matchUpdatedCh,
			newTermCh:        newTermCh,
			leaderUpdateCh:   make(chan leaderUpdate, 1),
			ldr:              r.String(),
		}
		repls[m.addr] = repl

		// send initial empty AppendEntries RPCs (heartbeat) to each follower
		req := &appendEntriesRequest{
			term:              r.term,
			leaderID:          r.addr,
			leaderCommitIndex: r.commitIndex,
			prevLogIndex:      r.lastLogIndex,
			prevLogTerm:       r.lastLogTerm,
		}
		// don't retry on failure. so that we can respond to apply/inspect
		debug(r, m.addr, ">> firstHeartbeat")
		_, _ = repl.appendEntries(req)

		// todo: should runLeader wait for repls to stop ?
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			repl.runLoop(req)
			debug(repl.ldr, repl.member.addr, "replicator closed")
		}()
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

			r.commitAndApplyOnMajority(repls)

		case ne := <-r.ApplyCh:
			r.storeNewEntry(ne, repls)

		case f := <-r.inspectCh:
			f(r)

		case <-leaseTimer.C:
			if !r.isQuorumReachable(repls) {
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
func (r *Raft) commitAndApplyOnMajority(repls map[string]*replicator) {
	majorityMatchIndex := r.lastLogIndex
	if len(r.members) > 1 {
		matched := make(decrUint64Slice, len(r.members))
		for i, m := range r.members {
			if m.addr == r.addr {
				matched[i] = r.lastLogIndex
			} else {
				matched[i] = repls[m.addr].matchedIndex
			}
		}
		// sort in decrease order
		sort.Sort(matched)
		majorityMatchIndex = matched[r.quorumSize()-1]
	}

	if majorityMatchIndex > r.commitIndex && majorityMatchIndex >= r.leaderTermStartIndex {
		r.commitIndex = majorityMatchIndex
		r.fsmApply(r.newEntries)
		r.notifyReplicators(repls) // we updated commit index
	}
}

func (r *Raft) storeNewEntry(ne NewEntry, repls map[string]*replicator) {
	if ne.entry == nil {
		ne.entry = &entry{}
	}
	ne.data, ne.Data = ne.Data, nil
	ne.index, ne.term = r.lastLogIndex+1, r.term

	// append entry to local log
	if ne.typ == entryNoop {
		debug(r, "log.append noop", ne.index)
	} else {
		debug(r, "log.append cmd", ne.index)
	}
	r.storage.append(ne.entry)
	r.lastLogIndex, r.lastLogTerm = ne.index, ne.term
	r.newEntries.PushBack(ne)

	// we updated lastLogIndex, so notify replicators
	if repls == nil {
		// no-op entry, repls have not yet created
		if len(r.members) == 1 {
			r.commitIndex = r.lastLogIndex
			r.fsmApply(r.newEntries)
		}
	} else {
		if len(r.members) == 1 {
			r.commitAndApplyOnMajority(repls)
		} else {
			r.notifyReplicators(repls)
		}
	}
}

func (r *Raft) isQuorumReachable(repls map[string]*replicator) bool {
	reachable := 0
	now := time.Now()
	for _, m := range r.members {
		if m.addr == r.addr {
			reachable++
		} else {
			repl := repls[m.addr]
			repl.noContactMu.RLock()
			noContact := repl.noContact
			repl.noContactMu.RUnlock()
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

func (r *Raft) notifyReplicators(repls map[string]*replicator) {
	leaderUpdate := leaderUpdate{
		lastIndex:   r.lastLogIndex,
		commitIndex: r.commitIndex,
	}
	for _, repl := range repls {
		select {
		case repl.leaderUpdateCh <- leaderUpdate:
		case <-repl.leaderUpdateCh:
			repl.leaderUpdateCh <- leaderUpdate
		}
	}
}

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
