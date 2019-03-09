package raft

import (
	"container/list"
	"fmt"
	"sort"
	"sync"
	"time"
)

const minCheckInterval = 10 * time.Millisecond

type ldrShip struct {
	*Raft

	// if quorum of nodes are not reachable for this duration
	// leader steps down to follower
	leaseTimer *time.Timer

	// leader term starts from this index.
	// this index refers to noop entry
	startIndex uint64

	// queue in which user submitted entries are enqueued
	// committed entries are dequeued and handed over to fsm go-routine
	newEntries *list.List

	// holds running replications, key is addr
	members map[ID]*member
	wg      sync.WaitGroup

	// to receive updates from replicators
	fromReplsCh chan interface{}
}

func (l *ldrShip) init() {
	assert(l.leader == l.id, "%s ldr.leader: got %s, want %s", l, l.leader, l.id)

	l.leaseTimer.Stop() // we start it on detecting failures
	l.startIndex = l.lastLogIndex + 1
	l.fromReplsCh = make(chan interface{}, len(l.configs.Latest.Nodes))

	// add a blank no-op entry into log at the start of its term
	l.storeEntry(NewEntry{
		entry: &entry{
			typ: entryNop,
		},
	})

	// start replication routine for each follower
	for _, node := range l.configs.Latest.Nodes {
		l.addMember(node)
	}
}

func (l *ldrShip) release() {
	if !l.leaseTimer.Stop() {
		select {
		case <-l.leaseTimer.C:
		default:
		}
	}

	for id, m := range l.members {
		close(m.stopCh)
		delete(l.members, id)
	}

	if l.leader == l.id {
		l.leader = ""
	}

	// respond to any pending user entries
	var err error
	if l.isClosed() {
		err = ErrServerClosed
	} else {
		err = NotLeaderError{l.leaderAddr(), true}
	}
	for l.newEntries.Len() > 0 {
		ne := l.newEntries.Remove(l.newEntries.Front()).(NewEntry)
		ne.reply(err)
	}

	// wait for replicators to finish
	l.wg.Wait()
	l.fromReplsCh = nil
}

func (l *ldrShip) storeEntry(ne NewEntry) {
	ne.entry.index, ne.entry.term = l.lastLogIndex+1, l.term

	// append entry to local log
	debug(l, "log.append", ne.typ, ne.index)
	if ne.typ != entryQuery && ne.typ != entryBarrier {
		l.storage.appendEntry(ne.entry)
	}
	l.newEntries.PushBack(ne)

	// we updated lastLogIndex, so notify replicators
	if ne.typ == entryQuery || ne.typ == entryBarrier {
		l.applyCommitted()
	} else {
		if ne.typ == entryConfig {
			config := Config{}
			err := config.decode(ne.entry)
			assert(err == nil, "BUG: %v", err)
			l.Raft.changeConfig(config)
		}
		l.notifyMembers(ne.typ == entryConfig)
	}
}

func (l *ldrShip) addMember(node Node) {
	m := &member{
		node:          node,
		rtime:         newRandTime(),
		status:        memberStatus{id: node.ID},
		ldrStartIndex: l.startIndex,
		connPool:      l.getConnPool(node.ID),
		hbTimeout:     l.hbTimeout,
		storage:       l.storage,
		stopCh:        make(chan struct{}),
		toLeaderCh:    l.fromReplsCh,
		fromLeaderCh:  make(chan leaderUpdate, 1),
		trace:         &l.trace,
		str:           fmt.Sprintf("%v %s", l, string(node.ID)),
	}
	l.members[node.ID] = m

	// send initial empty AppendEntries RPCs (heartbeat) to each follower
	req := &appendEntriesReq{
		term:           l.term,
		leader:         l.id,
		ldrCommitIndex: l.commitIndex,
		prevLogIndex:   l.lastLogIndex,
		prevLogTerm:    l.lastLogTerm,
	}

	l.wg.Add(1)
	if node.ID == l.id {
		go func() {
			// self replication: when leaderUpdate comes
			// just notify that it is replicated
			// we are doing this, so that the it is easier
			// to handle the case of single node cluster
			// todo: is this really needed? we can optimize it
			//       by avoiding this extra goroutine
			defer l.wg.Done()
			m.notifyLdr(matchIndex{&m.status, req.prevLogIndex})
			for {
				select {
				case <-m.stopCh:
					return
				case update := <-m.fromLeaderCh:
					m.notifyLdr(matchIndex{&m.status, update.lastIndex})
				}
			}
		}()
	} else {
		// don't retry on failure. so that we can respond to apply/inspect
		debug(m, ">> firstHeartbeat")
		_ = m.doRPC(req, &appendEntriesResp{})
		go func() {
			defer l.wg.Done()
			m.replicate(req)
			debug(m, "m.replicateEnd")
		}()
	}
}

func (l *ldrShip) checkReplUpdates(update interface{}) {
	matchUpdated, noContactUpdated := false, false
	for {
		switch update := update.(type) {
		case matchIndex:
			matchUpdated = true
			update.status.matchIndex = update.val
		case noContact:
			noContactUpdated = true
			update.status.noContact = update.time
			if l.trace.Unreachable != nil {
				l.trace.Unreachable(l.liveInfo(), update.status.id, update.time)
			}
		case newTerm:
			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			debug(l, "leader -> follower")
			l.state = Follower
			l.setTerm(update.val)
			l.leader = ""
			return
		case roundCompleted:
			round := update.round
			if round.id > update.status.rounds {
				debug(l, "roundCompleted", round)
				update.status.rounds++
				if l.trace.RoundCompleted != nil {
					l.trace.RoundCompleted(l.liveInfo(), update.status.id, round.id, round.duration(), round.lastIndex)
				}
			} else {
				debug(l, update.status.id, "is reminding promotion:", round)
			}
			if !l.configs.IsCommitted() {
				debug(l, "config not committed")
				break
			}
			hasNewEntries := l.lastLogIndex > update.status.matchIndex
			if hasNewEntries && round.duration() > l.promoteThreshold {
				debug(l, "best of luck for next round")
				break // best of luck for next round !!!
			}
			n, ok := l.configs.Latest.Nodes[update.status.id]
			if !ok || !n.promote() {
				debug(l, "this node should not be promoted")
				break
			}

			// promoting member
			debug(l, "promoting", n.ID)
			config := l.configs.Latest.clone()
			n.Voter, n.Promote = true, false
			config.Nodes[n.ID] = n
			if l.trace.Promoting != nil {
				l.trace.Promoting(l.liveInfo(), n.ID, round.id)
			}
			l.doChangeConfig(nil, config)
		}

		// get any waiting update
		select {
		case <-l.shutdownCh:
			return
		case update = <-l.fromReplsCh:
			continue
		default:
		}
		break
	}
	if matchUpdated {
		l.onMajorityCommit()
	}
	if noContactUpdated {
		l.checkLeaderLease()
	}
}

func (l *ldrShip) checkLeaderLease() {
	voters, reachable := 0, 0
	now, firstFailure := time.Now(), time.Time{}
	for _, node := range l.configs.Latest.Nodes {
		if node.Voter {
			voters++
			m := l.members[node.ID]
			noContact := m.status.noContact
			if noContact.IsZero() {
				reachable++
			} else if now.Sub(noContact) <= l.ldrLeaseTimeout {
				reachable++
				if firstFailure.IsZero() || noContact.Before(firstFailure) {
					firstFailure = noContact
				}
			}
		}
	}

	// todo: if quorum unreachable raise alert
	if reachable < voters/2+1 {
		debug(l, "leader -> follower quorumUnreachable")
		l.state = Follower
		l.leader = ""
		return
	}

	if !l.leaseTimer.Stop() {
		select {
		case <-l.leaseTimer.C:
		default:
		}
	}

	if !firstFailure.IsZero() {
		d := l.ldrLeaseTimeout - now.Sub(firstFailure)
		if d < minCheckInterval {
			d = minCheckInterval
		}
		l.leaseTimer.Reset(d)
	}
}

// computes N such that, a majority of matchIndex[i] ≥ N
func (l *ldrShip) majorityMatchIndex() uint64 {
	numVoters := l.configs.Latest.numVoters()
	if numVoters == 1 {
		for _, node := range l.configs.Latest.Nodes {
			if node.Voter {
				return l.members[node.ID].status.matchIndex
			}
		}
	}

	matched := make(decrUint64Slice, numVoters)
	i := 0
	for _, node := range l.configs.Latest.Nodes {
		if node.Voter {
			matched[i] = l.members[node.ID].status.matchIndex
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
func (l *ldrShip) onMajorityCommit() {
	majorityMatchIndex := l.majorityMatchIndex()

	// note: if majorityMatchIndex >= ldr.startIndex, it also mean
	// majorityMatchIndex.term == currentTerm
	if majorityMatchIndex > l.commitIndex && majorityMatchIndex >= l.startIndex {
		l.setCommitIndex(majorityMatchIndex)
		l.applyCommitted()
		l.notifyMembers(false) // we updated commit index
	}
}

// if commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
func (l *ldrShip) applyCommitted() {
	for {
		// send query/barrier entries if any to fsm
		for l.newEntries.Len() > 0 {
			elem := l.newEntries.Front()
			ne := elem.Value.(NewEntry)
			if ne.index == l.lastApplied+1 && (ne.typ == entryQuery || ne.typ == entryBarrier) {
				l.newEntries.Remove(elem)
				debug(l, "fms <- {", ne.typ, ne.index, "}")
				select {
				case <-l.shutdownCh:
					ne.reply(ErrServerClosed)
					return
				case l.fsmTaskCh <- ne:
				}
			} else {
				break
			}
		}

		if l.lastApplied+1 > l.commitIndex {
			return
		}

		// get lastApplied+1 entry
		var ne NewEntry
		if l.newEntries.Len() > 0 {
			elem := l.newEntries.Front()
			if elem.Value.(NewEntry).index == l.lastApplied+1 {
				ne = l.newEntries.Remove(elem).(NewEntry)
			}
		}
		if ne.entry == nil {
			ne.entry = &entry{}
			l.storage.getEntry(l.lastApplied+1, ne.entry)
		}

		l.applyEntry(ne)
		l.lastApplied++
		debug(l, "lastApplied", l.lastApplied)
	}
}

func (l *ldrShip) notifyMembers(includeConfig bool) {
	update := leaderUpdate{
		lastIndex:   l.lastLogIndex,
		commitIndex: l.commitIndex,
	}
	if includeConfig {
		update.config = &l.configs.Latest
	}
	for _, m := range l.members {
		select {
		case m.fromLeaderCh <- update:
		case <-m.fromLeaderCh:
			m.fromLeaderCh <- update
		}
	}
}

// -------------------------------------------------------

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
