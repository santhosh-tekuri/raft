package raft

import (
	"container/list"
	"errors"
	"fmt"
	"sort"
	"time"
)

func (r *Raft) runLeader() {
	ldr := leadership{
		Raft:         r,
		leaseTimeout: r.heartbeatTimeout, // todo: should it be same as heartbeatTimeout ?
		newEntries:   list.New(),
		repls:        make(map[NodeID]*replication),
	}
	ldr.runLoop()
}

// ----------------------------------------------

type replUpdate struct {
	status     *replStatus
	matchIndex uint64
	noContact  time.Time
}

type replStatus struct {
	// owned exclusively by raft main goroutine
	// used to recalculateMatch
	matchIndex uint64

	// from what time the replication unable to reach this node
	// zero value means it is reachable
	noContact time.Time
}

// did we have success full contact after time t
func (rs *replStatus) contactedAfter(t time.Time) bool {
	return rs.noContact.IsZero() || rs.noContact.After(t)
}

// -------------------------------------------------------

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

	// holds running replications, key is addr
	repls map[NodeID]*replication

	// to receive new term notifications from replicators
	newTermCh chan uint64

	// to receive updates from replicators
	replUpdatedCh chan replUpdate
}

func (ldr *leadership) runLoop() {
	assert(ldr.leader == ldr.addr, "%s ldr.leader: got %s, want %s", ldr, ldr.leader, ldr.addr)

	ldr.startIndex = ldr.lastLogIndex + 1

	// add a blank no-op entry into log at the start of its term
	ldr.applyEntry(newEntry{
		entry: &entry{
			typ: entryNoop,
		},
	})

	ldr.newTermCh = make(chan uint64, len(ldr.configs.Latest.Nodes))
	ldr.replUpdatedCh = make(chan replUpdate, len(ldr.configs.Latest.Nodes))

	defer func() {
		for _, repl := range ldr.repls {
			close(repl.stopCh)
		}

		if ldr.leader == ldr.addr {
			ldr.leader = ""
		}

		// respond to any pending user entries
		for e := ldr.newEntries.Front(); e != nil; e = e.Next() {
			e.Value.(newEntry).task.reply(NotLeaderError{ldr.leader})
		}
	}()

	// start replication routine for each follower

	for _, node := range ldr.configs.Latest.Nodes {
		ldr.startReplication(node)
	}

	leaseTimer := time.NewTicker(ldr.leaseTimeout)
	defer leaseTimer.Stop()

	for ldr.state == Leader {
		select {
		case <-ldr.shutdownCh:
			return

		case newTerm := <-ldr.newTermCh:
			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			debug(ldr, "leader -> follower")
			ldr.state = Follower
			ldr.setTerm(newTerm)
			ldr.leader = ""
			StateChanged(ldr.Raft, ldr.state)
			return

		case rpc := <-ldr.rpcCh:
			ldr.replyRPC(rpc)

		case replUpdate := <-ldr.replUpdatedCh:
		loop:
			// get pending repl updates
			for {
				replUpdate.status.matchIndex = replUpdate.matchIndex
				replUpdate.status.noContact = replUpdate.noContact
				select {
				case <-ldr.shutdownCh:
					return
				case replUpdate = <-ldr.replUpdatedCh:
					break
				default:
					break loop
				}
			}

			ldr.commitAndApplyOnMajority()

		case t := <-ldr.TasksCh:
			ldr.executeTask(t)

		case <-leaseTimer.C:
			t := time.Now().Add(-ldr.leaseTimeout)
			if !ldr.isQuorumReachable(t) {
				debug(ldr, "quorumUnreachable")
				debug(ldr, "leader -> follower")
				ldr.state = Follower
				ldr.leader = ""
				StateChanged(ldr.Raft, ldr.state)
			}
		}
	}
}

func (ldr *leadership) startReplication(node Node) {
	repl := &replication{
		status:           replStatus{},
		connPool:         ldr.getConnPool(node.Addr),
		heartbeatTimeout: ldr.heartbeatTimeout,
		storage:          ldr.storage,
		stopCh:           make(chan struct{}),
		matchUpdatedCh:   ldr.replUpdatedCh,
		newTermCh:        ldr.newTermCh,
		leaderUpdateCh:   make(chan leaderUpdate, 1),
		str:              ldr.String() + " " + string(node.ID),
	}
	ldr.repls[node.ID] = repl

	// send initial empty AppendEntries RPCs (heartbeat) to each follower
	req := &appendEntriesRequest{
		term:              ldr.term,
		leaderID:          ldr.addr,
		leaderCommitIndex: ldr.commitIndex,
		prevLogIndex:      ldr.lastLogIndex,
		prevLogTerm:       ldr.lastLogTerm,
	}

	// todo: should runLeader wait for repls to stop ?
	ldr.wg.Add(1)
	if node.ID == ldr.id {
		go func() {
			// self replication: when leaderUpdate comes
			// just notify that it is replicated
			// we are doing this, so that the it is easier
			// to handle the case of single node cluster
			// todo: is this really needed? we can optimize it
			//       by avoiding this extra goroutine
			defer ldr.wg.Done()
			repl.notifyLdr(req.prevLogIndex, time.Time{})
			for {
				select {
				case <-repl.stopCh:
					return
				case update := <-repl.leaderUpdateCh:
					repl.notifyLdr(update.lastIndex, time.Time{})
				}
			}
		}()
	} else {
		// don't retry on failure. so that we can respond to apply/inspect
		debug(repl, ">> firstHeartbeat")
		_, _ = repl.appendEntries(req)
		go func() {
			defer ldr.wg.Done()
			repl.runLoop(req)
			debug(repl, "repl.end")
		}()
	}
}

func (ldr *leadership) applyEntry(ne newEntry) {
	ne.entry.index, ne.entry.term = ldr.lastLogIndex+1, ldr.term

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

// is quorum of nodes reachable after time t
func (ldr *leadership) isQuorumReachable(t time.Time) bool {
	voters, reachable := 0, 0
	for _, node := range ldr.configs.Latest.Nodes {
		if node.Type == Voter {
			voters++
			repl := ldr.repls[node.ID]
			if repl.status.contactedAfter(t) {
				reachable++
			}
		}
	}
	// todo: if quorum unreachable raise alert
	return reachable >= voters/2+1
}

// computes N such that, a majority of matchIndex[i] ≥ N
func (ldr *leadership) majorityMatchIndex() uint64 {
	numVoters := ldr.configs.Latest.numVoters()
	if numVoters == 1 {
		for _, node := range ldr.configs.Latest.Nodes {
			if node.Type == Voter {
				return ldr.repls[node.ID].status.matchIndex
			}
		}
	}

	matched := make(decrUint64Slice, numVoters)
	i := 0
	for _, node := range ldr.configs.Latest.Nodes {
		if node.Type == Voter {
			matched[i] = ldr.repls[node.ID].status.matchIndex
			i++
		}
	}
	// sort in decrease order
	sort.Sort(matched)
	quorum := numVoters/2 + 1
	return matched[quorum-1]
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

func (ldr *leadership) addNode(t addNode) {
	if !ldr.configs.IsCommitted() {
		t.reply(errors.New("raft: configChange is in progress"))
	}
	if ldr.commitIndex < ldr.startIndex {
		t.reply(errors.New("raft: noop entry is not yet committed"))
	}
	if _, ok := ldr.configs.Latest.Nodes[t.node.ID]; ok {
		t.reply(fmt.Errorf("raft: node %s already exists", t.node.ID))
	}
	newConfig := ldr.configs.Latest.clone()
	newConfig.Nodes[t.node.ID] = t.node
	ldr.applyConfig(newConfig)
	t.reply(nil)
}

func (ldr *leadership) applyConfig(newConfig Config) {
	ne := newEntry{
		entry: newConfig.encode(),
	}
	ldr.applyEntry(ne)
	debug(ldr, "XXXXXXXXXXXXXXXXxx", ne.index, ne.term)
	newConfig.Index, newConfig.Term = ne.index, ne.term
	ldr.configs.Latest = newConfig
	ldr.storage.setConfigs(ldr.configs)
}

// -------------------------------------------------------

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
