package raft

import (
	"container/list"
	"time"
)

func (r *Raft) runLeader() {
	ldr := &leaderState{
		Raft:         r,
		leaseTimeout: r.heartbeatTimeout, // todo: should it be same as heartbeatTimeout ?
		newEntries:   list.New(),
	}
	ldr.runLoop()
}

type leaderState struct {
	*Raft

	// if quorum of nodes are not reachable for this duration
	// leader steps down to follower
	leaseTimeout time.Duration

	// leader term starts from this index.
	// this index refers to noop entry
	startIndex uint64

	// queue in which user submitted entries are enqueued
	// committed entries are dequeued and handed over to fsm go-routine
	newEntries *list.List

	// holds running replications, key is addr
	repls map[string]*replication
}

func (ldr *leaderState) runLoop() {
	assert(ldr.leaderID == ldr.addr, "%s ldr.leaderID: got %s, want %s", ldr, ldr.leaderID, ldr.addr)

	ldr.startIndex = ldr.lastLogIndex + 1

	// add a blank no-op entry into log at the start of its term
	ldr.storeNewEntry(newEntry{
		entry: &entry{
			typ: entryNoop,
		},
	})

	// to receive new term notifications from replicators
	newTermCh := make(chan uint64, len(ldr.config.members()))

	// to receive matchIndex updates from replicators
	matchUpdatedCh := make(chan *replication, len(ldr.config.members()))

	// to send stop signal to replicators
	stopReplsCh := make(chan struct{})

	defer func() {
		close(stopReplsCh)

		if ldr.leaderID == ldr.addr {
			ldr.leaderID = ""
		}

		// respond to any pending user entries
		for e := ldr.newEntries.Front(); e != nil; e = e.Next() {
			e.Value.(newEntry).task.reply(NotLeaderError{ldr.leaderID})
		}
	}()

	// start replication routine for each follower
	ldr.repls = make(map[string]*replication)
	for _, m := range ldr.config.members() {
		// matchIndex initialized to zero
		m.matchIndex = 0 // todo: should we reset always to zero?
		repl := &replication{
			member:           m,
			heartbeatTimeout: ldr.heartbeatTimeout,
			storage:          ldr.storage,
			nextIndex:        ldr.lastLogIndex + 1, // nextIndex initialized to leader last log index + 1
			matchIndex:       m.matchIndex,
			stopCh:           stopReplsCh,
			matchUpdatedCh:   matchUpdatedCh,
			newTermCh:        newTermCh,
			leaderUpdateCh:   make(chan leaderUpdate, 1),
			str:              ldr.String() + " " + m.addr,
		}
		ldr.repls[m.addr] = repl

		// send initial empty AppendEntries RPCs (heartbeat) to each follower
		req := &appendEntriesRequest{
			term:              ldr.term,
			leaderID:          ldr.addr,
			leaderCommitIndex: ldr.commitIndex,
			prevLogIndex:      ldr.lastLogIndex,
			prevLogTerm:       ldr.lastLogTerm,
		}

		if m.addr != ldr.addr {
			// don't retry on failure. so that we can respond to apply/inspect
			debug(repl, ">> firstHeartbeat")
			_, _ = repl.appendEntries(req)
		}

		// todo: should runLeader wait for repls to stop ?
		ldr.wg.Add(1)
		go func() {
			defer ldr.wg.Done()
			repl.runLoop(req)
			debug(repl, "replication closed")
		}()
	}

	leaseTimer := time.NewTicker(ldr.leaseTimeout)
	defer leaseTimer.Stop()

	for ldr.state == leader {
		select {
		case <-ldr.shutdownCh:
			return

		case newTerm := <-newTermCh:
			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			debug(ldr, "leader -> follower")
			ldr.state = follower
			ldr.setTerm(newTerm)
			ldr.leaderID = ""
			stateChanged(ldr.Raft)
			return

		case rpc := <-ldr.rpcCh:
			ldr.replyRPC(rpc)

		case m := <-matchUpdatedCh:
		loop:
			// get latest matchIndex from all notified members
			for {
				m.member.matchIndex = m.getMatchIndex()
				select {
				case m = <-matchUpdatedCh:
					break
				default:
					break loop
				}
			}

			ldr.commitAndApplyOnMajority()

		case t := <-ldr.TasksCh:
			ldr.storeNewEntry(newEntry{
				entry: &entry{
					typ:  entryCommand,
					data: t.Command.([]byte),
				},
				task: t,
			})

		case f := <-ldr.inspectCh:
			f(ldr.Raft)

		case <-leaseTimer.C:
			t := time.Now().Add(-ldr.leaseTimeout)
			if !ldr.config.isQuorumReachable(t) {
				debug(ldr, "quorumUnreachable")
				ldr.state = follower
				ldr.leaderID = ""
				stateChanged(ldr.Raft)
			}
		}
	}
}

func (ldr *leaderState) storeNewEntry(ne newEntry) {
	ne.index, ne.term = ldr.lastLogIndex+1, ldr.term

	// append entry to local log
	if ne.typ == entryNoop {
		debug(ldr, "log.append noop", ne.index)
	} else {
		debug(ldr, "log.append cmd", ne.index)
	}
	ldr.storage.append(ne.entry)
	ldr.lastLogIndex, ldr.lastLogTerm = ne.index, ne.term
	ldr.newEntries.PushBack(ne)

	// we updated lastLogIndex, so notify replicators
	ldr.notifyReplicators()
}

// If majorityMatchIndex(N) > commitIndex,
// and log[N].term == currentTerm: set commitIndex = N
func (ldr *leaderState) commitAndApplyOnMajority() {
	majorityMatchIndex := ldr.config.majorityMatchIndex()

	// note: if majorityMatchIndex >= ldr.startIndex, it also mean
	// majorityMatchIndex.term == currentTerm
	if majorityMatchIndex > ldr.commitIndex && majorityMatchIndex >= ldr.startIndex {
		ldr.commitIndex = majorityMatchIndex
		debug(ldr, "commitIndex", ldr.commitIndex)
		ldr.fsmApply(ldr.newEntries)
		ldr.notifyReplicators() // we updated commit index
	}
}

func (ldr *leaderState) notifyReplicators() {
	leaderUpdate := leaderUpdate{
		lastIndex:   ldr.lastLogIndex,
		commitIndex: ldr.commitIndex,
	}
	for _, repl := range ldr.repls {
		select {
		case repl.leaderUpdateCh <- leaderUpdate:
		case <-repl.leaderUpdateCh:
			repl.leaderUpdateCh <- leaderUpdate
		}
	}
}
