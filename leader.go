package raft

import (
	"container/list"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const minCheckInterval = 10 * time.Millisecond

func (r *Raft) runLeader() {
	ldr := leadership{
		Raft:         r,
		leaseTimeout: r.hbTimeout, // todo: should it be same as heartbeatTimeout ? make configurable
		leaseTimer:   time.NewTimer(time.Hour),
		newEntries:   list.New(),
		repls:        make(map[NodeID]*replication),
	}
	ldr.leaseTimer.Stop() // we start it on detecting failures
	ldr.runLoop()
}

// ----------------------------------------------

type replUpdate struct {
	status     *replStatus
	matchIndex uint64
	noContact  time.Time
}

type replStatus struct {
	id NodeID

	// owned exclusively by leader goroutine
	// used to compute majorityMatchIndex
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
	leaseTimer   *time.Timer

	// leader term starts from this index.
	// this index refers to noop entry
	startIndex uint64

	// queue in which user submitted entries are enqueued
	// committed entries are dequeued and handed over to fsm go-routine
	newEntries *list.List

	// holds running replications, key is addr
	repls map[NodeID]*replication
	wg    sync.WaitGroup

	// to receive new term notifications from replicators
	newTermCh chan uint64

	// to receive updates from replicators
	replUpdatedCh chan replUpdate
}

func (ldr *leadership) runLoop() {
	assert(ldr.leader == ldr.addr, "%s ldr.leader: got %s, want %s", ldr, ldr.leader, ldr.addr)

	ldr.startIndex = ldr.lastLogIndex + 1

	// add a blank no-op entry into log at the start of its term
	ldr.applyEntry(NewEntry{
		entry: &entry{
			typ: entryNop,
		},
	})

	ldr.newTermCh = make(chan uint64, len(ldr.configs.Latest.Nodes))
	ldr.replUpdatedCh = make(chan replUpdate, len(ldr.configs.Latest.Nodes))

	defer func() {
		ldr.leaseTimer.Stop()
		for _, repl := range ldr.repls {
			close(repl.stopCh)
		}

		if ldr.leader == ldr.addr {
			ldr.leader = ""
		}

		// respond to any pending user entries
		for e := ldr.newEntries.Front(); e != nil; e = e.Next() {
			e.Value.(NewEntry).task.reply(NotLeaderError{ldr.leader})
		}
		ldr.wg.Wait()
	}()

	// start replication routine for each follower

	for _, node := range ldr.configs.Latest.Nodes {
		ldr.startReplication(node)
	}

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
			ldr.stateChanged()
			return

		case rpc := <-ldr.rpcCh:
			ldr.replyRPC(rpc)

		case replUpdate := <-ldr.replUpdatedCh:
			matchUpdated, noContactUpdated := false, false
		loop:
			// get pending repl updates
			for {
				if replUpdate.status.matchIndex != replUpdate.matchIndex {
					matchUpdated = true
					replUpdate.status.matchIndex = replUpdate.matchIndex
				}
				if !replUpdate.status.noContact.Equal(replUpdate.noContact) {
					noContactUpdated = true
					replUpdate.status.noContact = replUpdate.noContact
				}
				select {
				case <-ldr.shutdownCh:
					return
				case replUpdate = <-ldr.replUpdatedCh:
					break
				default:
					break loop
				}
			}
			if matchUpdated {
				ldr.commitAndApplyOnMajority()
			}
			if noContactUpdated {
				ldr.checkLeaderLease()
			}

		case <-ldr.leaseTimer.C:
			ldr.checkLeaderLease()

		case ne := <-ldr.Raft.newEntryCh:
			ldr.applyEntry(ne)

		case t := <-ldr.taskCh:
			ldr.executeTask(t)
		}
	}
}

func (ldr *leadership) startReplication(node Node) {
	repl := &replication{
		status:           replStatus{id: node.ID},
		connPool:         ldr.getConnPool(node.Addr),
		heartbeatTimeout: ldr.hbTimeout,
		storage:          ldr.storage,
		stopCh:           make(chan struct{}),
		replUpdatedCh:    ldr.replUpdatedCh,
		newTermCh:        ldr.newTermCh,
		ldrUpdateCh:      make(chan leaderUpdate, 1),
		str:              fmt.Sprintf("%v %s", ldr, string(node.ID)),
	}
	ldr.repls[node.ID] = repl

	// send initial empty AppendEntries RPCs (heartbeat) to each follower
	req := &appendEntriesRequest{
		term:           ldr.term,
		leaderID:       ldr.addr,
		ldrCommitIndex: ldr.commitIndex,
		prevLogIndex:   ldr.lastLogIndex,
		prevLogTerm:    ldr.lastLogTerm,
	}

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
				case update := <-repl.ldrUpdateCh:
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

func (ldr *leadership) applyEntry(ne NewEntry) {
	ne.entry.index, ne.entry.term = ldr.lastLogIndex+1, ldr.term

	// append entry to local log
	debug(ldr, "log.append", ne.typ, ne.index)
	if ne.typ != entryQuery {
		ldr.storage.append(ne.entry)
		ldr.lastLogIndex, ldr.lastLogTerm = ne.index, ne.term
	}
	ldr.newEntries.PushBack(ne)

	// we updated lastLogIndex, so notify replicators
	if ne.typ == entryQuery {
		// if all log entries are applied
		if ldr.lastApplied == ldr.lastLogIndex {
			ldr.fsmApply(ldr.newEntries)
		}
	} else {
		ldr.notifyReplicators()
	}
}

func (ldr *leadership) checkLeaderLease() {
	voters, reachable := 0, 0
	now, firstFailure := time.Now(), time.Time{}
	for _, node := range ldr.configs.Latest.Nodes {
		if node.Type == Voter {
			voters++
			repl := ldr.repls[node.ID]
			noContact := repl.status.noContact
			if noContact.IsZero() {
				reachable++
			} else if now.Sub(noContact) <= ldr.leaseTimeout {
				reachable++
				if firstFailure.IsZero() || noContact.Before(firstFailure) {
					firstFailure = noContact
				}
			}
		}
	}

	// todo: if quorum unreachable raise alert
	if reachable < voters/2+1 {
		debug(ldr, "leader -> follower quorumUnreachable")
		ldr.state = Follower
		ldr.leader = ""
		ldr.stateChanged()
		return
	}

	if !ldr.leaseTimer.Stop() {
		select {
		case <-ldr.leaseTimer.C:
		default:
		}
	}

	if !firstFailure.IsZero() {
		d := ldr.leaseTimeout - now.Sub(firstFailure)
		if d < minCheckInterval {
			d = minCheckInterval
		}
		ldr.leaseTimer.Reset(d)
	}
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

		// is latest config committed ?
		shutdown := false
		if !ldr.configs.IsCommitted() && ldr.commitIndex >= ldr.configs.Latest.Index {
			ldr.configs.Committed = ldr.configs.Latest
			ldr.storage.setConfigs(ldr.configs)

			// reply configEntries immediately
			// we cant reply in fsmLoop, because
			// in we are not part of cluster, we shutdown
			// immediately
			elem := ldr.newEntries.Front()
			for {
				ne := elem.Value.(NewEntry)
				if ne.index == ldr.configs.Latest.Index {
					ne.reply(nil)
					ldr.newEntries.Remove(elem)
					break
				} else if ne.index > ldr.configs.Latest.Index {
					// configEntry not found. means
					// it is submitted in earlier term
					break
				}
				elem = elem.Next()
			}

			if !ldr.configs.Latest.isVoter(ldr.id) {
				shutdown = true
			}
		}

		ldr.fsmApply(ldr.newEntries)
		ldr.notifyReplicators() // we updated commit index
		if shutdown {
			// if we don't shutdown, we will become follower
			// on heartbeat timeout, we notice that we are no longer
			// part of committed cluster. thus does not start election
			// and sits idle for ever
			ldr.Shutdown()
		}
	}
}

func (ldr *leadership) notifyReplicators() {
	leaderUpdate := leaderUpdate{
		lastIndex:   ldr.lastLogIndex,
		commitIndex: ldr.commitIndex,
	}
	for _, repl := range ldr.repls {
		select {
		case repl.ldrUpdateCh <- leaderUpdate:
		case <-repl.ldrUpdateCh:
			repl.ldrUpdateCh <- leaderUpdate
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
	ne := NewEntry{
		entry: newConfig.encode(),
	}
	ldr.applyEntry(ne)
	debug(ldr, "XXXXXXXXXXXXXXXXxx", ne.index, ne.term)
	newConfig.Index, newConfig.Term = ne.index, ne.term
	ldr.configs.Latest = newConfig
	ldr.storage.setConfigs(ldr.configs)

	// now majority might have changed. needs to be recalculated
	ldr.commitAndApplyOnMajority()
}

// -------------------------------------------------------

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
