package raft

import (
	"container/list"
	"sort"
	"time"
)

func (r *Raft) runLeader() {
	r.leadership = &leadership{
		Raft:         r,
		leaseTimeout: r.heartbeatTimeout, // todo: should it be same as heartbeatTimeout ?
		newEntries:   list.New(),
	}
	r.leadership.runLoop()
	r.leadership = nil
}

type leadership struct {
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

	voters map[string]*member

	// holds running replications, key is addr
	repls map[string]*replication
}

func (ldr *leadership) runLoop() {
	assert(ldr.leaderID == ldr.addr, "%s ldr.leaderID: got %s, want %s", ldr, ldr.leaderID, ldr.addr)

	ldr.voters = make(map[string]*member)
	for _, node := range ldr.configs.latest.nodes {
		if node.voter {
			ldr.voters[node.addr] = &member{
				addr:       node.addr,
				connPool:   ldr.getConnPool(node.addr), // todo: if we get connpool in runleader can we remove connpoolsMu in raft ?
				matchIndex: 0,                          // matchIndex initialized to zero
			}
		}
	}

	ldr.startIndex = ldr.lastLogIndex + 1

	// add a blank no-op entry into log at the start of its term
	ApplyEntry(nil).execute(ldr.Raft)

	// to receive new term notifications from replicators
	newTermCh := make(chan uint64, len(ldr.voters))

	// to receive matchIndex updates from replicators
	matchUpdatedCh := make(chan *replication, len(ldr.voters))

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
	for _, m := range ldr.voters {
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
			StateChanged(ldr.Raft, byte(ldr.state))
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
			t.execute(ldr.Raft)

		case <-leaseTimer.C:
			t := time.Now().Add(-ldr.leaseTimeout)
			if !ldr.isQuorumReachable(t) {
				debug(ldr, "quorumUnreachable")
				ldr.state = follower
				ldr.leaderID = ""
				StateChanged(ldr.Raft, byte(ldr.state))
			}
		}
	}
}

func (ldr *leadership) quorum() int {
	return len(ldr.voters)/2 + 1
}

// is quorum of nodes reachable after time t
func (ldr *leadership) isQuorumReachable(t time.Time) bool {
	reachable := 0
	for _, v := range ldr.voters {
		if v.contactedAfter(t) {
			reachable++
		}
	}
	return reachable >= ldr.quorum()
}

// computes N such that, a majority of matchIndex[i] ≥ N
func (ldr *leadership) majorityMatchIndex() uint64 {
	if len(ldr.voters) == 1 {
		for _, v := range ldr.voters {
			return v.matchIndex
		}
	}

	matched := make(decrUint64Slice, len(ldr.voters))
	i := 0
	for _, v := range ldr.voters {
		matched[i] = v.matchIndex
		i++
	}
	// sort in decrease order
	sort.Sort(matched)
	return matched[ldr.quorum()-1]
}

// If majorityMatchIndex(N) > commitIndex,
// and log[N].term == currentTerm: set commitIndex = N
func (ldr *leadership) commitAndApplyOnMajority() {
	majorityMatchIndex := ldr.majorityMatchIndex()

	// note: if majorityMatchIndex >= ldr.startIndex, it also mean
	// majorityMatchIndex.term == currentTerm
	if majorityMatchIndex > ldr.commitIndex && majorityMatchIndex >= ldr.startIndex {
		ldr.commitIndex = majorityMatchIndex
		debug(ldr, "commitIndex", ldr.commitIndex)
		ldr.fsmApply(ldr.newEntries)
		ldr.notifyReplicators() // we updated commit index
	}
}

func (ldr *leadership) notifyReplicators() {
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

// -------------------------------------------------------

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
