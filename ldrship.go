package raft

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type ldrShip struct {
	*Raft

	voter bool

	// leader term starts from this index.
	// this index refers to noop entry
	startIndex uint64

	// queue in which user submitted entries are enqueued
	// committed entries are dequeued and handed over to fsm go-routine
	neHead, neTail *newEntry

	// holds running replications, key is addr
	flrs map[uint64]*flr
	wg   sync.WaitGroup

	// to receive updates from replicators
	fromReplsCh chan interface{} // todo: can we have some buffer ??

	transfer   transfer
	waitStable []waitForStableConfig

	removeLTE uint64
}

func (l *ldrShip) init() {
	assert(l.leader == l.nid, "%v ldr.leader: got %d, want %d", l, l.leader, l.nid)

	l.voter = true
	l.startIndex = l.lastLogIndex + 1
	l.fromReplsCh = make(chan interface{}, 1024)
	l.removeLTE = l.log.PrevIndex()

	// start replication routine for each follower
	for id, n := range l.configs.Latest.Nodes {
		if id != l.nid {
			l.addFlr(n)
		}
	}
	l.onActionChange()

	// add a blank no-op entry into log at the start of its term
	l.storeEntry(&newEntry{entry: &entry{typ: entryNop}})
}

func (l *ldrShip) onTimeout() { l.checkQuorum(0) }

func (l *ldrShip) release() {
	if l.transfer.inProgress() {
		var err error
		if l.term > l.transfer.term {
			err = nil
		} else if l.isClosed() {
			err = ErrServerClosed
		} else {
			err = ErrQuorumUnreachable
		}
		l.transfer.reply(err)
	}

	debug(l, "stopping followers")
	for id, f := range l.flrs {
		close(f.stopCh)
		delete(l.flrs, id)
	}
	if l.leader == l.nid {
		l.setLeader(0)
	}

	// respond to any pending user entries
	var err error = NotLeaderError{l.leader, l.leaderAddr(), true}
	if l.isClosed() {
		err = ErrServerClosed
	}
	for ne := l.neHead; ne != nil; ne = ne.next {
		ne.reply(err)
	}
	l.neHead, l.neTail = nil, nil
	for buffed := len(l.fsmTaskCh); buffed > 0; buffed-- {
		t := <-l.fsmTaskCh
		t.reply(err)
	}

	for _, t := range l.waitStable {
		t.reply(err)
	}
	l.waitStable = nil

	// wait for replicators to finish
	l.wg.Wait()
	l.fromReplsCh = nil
}

func (l *ldrShip) storeEntry(ne *newEntry) {
	i, lastIndex, configIndex := 0, l.lastLogIndex, l.configs.Latest.Index
	for {
		i++
		if l.transfer.inProgress() {
			ne.reply(InProgressError("transferLeadership"))
		} else {
			ne.entry.index, ne.entry.term = l.lastLogIndex+1, l.term
			if l.neTail != nil {
				l.neTail.next, l.neTail = ne, ne
			} else {
				l.neHead, l.neTail = ne, ne
			}
			if ne.isLogEntry() {
				debug(l, "log.append", ne.typ, ne.index)
				l.storage.appendEntry(ne.entry)
				if ne.typ == entryConfig {
					config := Config{}
					if err := config.decode(ne.entry); err != nil {
						panic(bug(1, "config.decode: %v", err))
					}
					l.changeConfig(config)
				}
			}
		}
		if i < maxAppendEntries {
			select {
			case t := <-l.fsmTaskCh:
				ne = t.newEntry()
				continue
			default:
			}
		}
		break
	}
	if l.neHead != nil && !l.neHead.isLogEntry() {
		l.applyCommitted()
	}
	if l.lastLogIndex > lastIndex {
		debug(l, "syncLog", l.lastLogIndex-lastIndex, "entries")
		l.storage.syncLog()
		l.beginFinishedRounds()
		l.notifyFlr(l.configs.Latest.Index > configIndex)
	}
}

func (l *ldrShip) addFlr(n Node) {
	assert(n.ID != l.nid, "adding leader as follower")
	f := &flr{
		voter:         n.Voter,
		rtime:         newRandTime(),
		status:        flrStatus{id: n.ID, removeLTE: l.removeLTE},
		ldrStartIndex: l.startIndex,
		ldrLastIndex:  l.lastLogIndex,
		matchIndex:    0,
		nextIndex:     l.lastLogIndex + 1,
		connPool:      l.getConnPool(n.ID),
		hbTimeout:     l.hbTimeout,
		timer:         newSafeTimer(),
		log:           l.storage.log.ViewAt(l.removeLTE, l.lastLogIndex),
		snaps:         l.storage.snaps,
		stopCh:        make(chan struct{}),
		toLeaderCh:    l.fromReplsCh,
		fromLeaderCh:  make(chan leaderUpdate, 1),
		str:           fmt.Sprintf("%v M%d", l, n.ID),
	}
	l.flrs[n.ID] = f

	// send initial empty AppendEntries RPCs (heartbeat) to each follower
	req := &appendEntriesReq{
		req:            req{l.term, l.nid},
		ldrCommitIndex: l.commitIndex,
		prevLogIndex:   l.lastLogIndex,
		prevLogTerm:    l.lastLogTerm,
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		f.runLoop(req)
		debug(f, "f.replicateEnd")
	}()
}

func (l *ldrShip) checkReplUpdates(u interface{}) {
	matchUpdated, noContactUpdated, removeLTEUpdated := false, false, false
	for {
		debug(l, "<<", u)
		switch u := u.(type) {
		case error:
			panic(u)
		case matchIndex:
			matchUpdated = true
			u.status.matchIndex = u.val
			l.checkActionStatus(u.status)
		case removeLTE:
			removeLTEUpdated = true
			u.status.removeLTE = u.val
		case noContact:
			noContactUpdated = true
			u.status.noContact, u.status.err = u.time, u.err
			if l.trace.Unreachable != nil {
				l.trace.Unreachable(l.liveInfo(), u.status.id, u.time, u.err)
			}
		case newTerm:
			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			l.setState(Follower)
			l.setLeader(0)
			l.setTerm(u.val)
			return
		}

		// get any waiting update
		select {
		case <-l.close:
			return
		case u = <-l.fromReplsCh:
			continue
		default:
		}
		break
	}
	if matchUpdated {
		l.onMajorityCommit()
	}
	if noContactUpdated {
		l.checkQuorum(l.quorumWait)
	}
	if removeLTEUpdated && l.removeLTE > l.log.PrevIndex() {
		l.checkLogCompact()
	}

	// todo: do this in case matchIndex in above switch
	if matchUpdated || noContactUpdated {
		if l.transfer.inProgress() && !l.transfer.targetChosen() {
			l.tryTransfer()
		}
	}
}

func (l *ldrShip) checkQuorum(wait time.Duration) {
	voters, reachable := 0, 0
	for id, n := range l.configs.Latest.Nodes {
		if n.Voter {
			voters++
			if id == l.nid || l.flrs[id].status.noContact.IsZero() {
				reachable++
			}
		}
	}

	if reachable >= voters/2+1 {
		if l.timer.active {
			debug(l, "quorumReachable")
			if l.trace.QuorumUnreachable != nil {
				l.trace.QuorumUnreachable(l.liveInfo(), time.Time{})
			}
			l.timer.stop()
		}
		return
	}

	if l.quorumWait == 0 || !l.timer.active {
		if l.trace.QuorumUnreachable != nil {
			l.trace.QuorumUnreachable(l.liveInfo(), time.Now())
		}
	}
	if wait == 0 {
		l.setState(Follower)
		l.setLeader(0)
	} else if !l.timer.active {
		debug(l, "quorumUnreachable: waiting", wait)
		l.timer.reset(wait)
	}
}

// computes N such that, a majority of matchIndex[i] ≥ N
func (l *ldrShip) majorityMatchIndex() uint64 {
	matched := make(decrUint64Slice, len(l.configs.Latest.Nodes))
	i := 0
	for _, n := range l.configs.Latest.Nodes {
		if n.Voter {
			if n.ID == l.nid {
				matched[i] = l.lastLogIndex
			} else {
				matched[i] = l.flrs[n.ID].status.matchIndex
			}
			i++
		}
	}
	// sort in decrease order
	sort.Sort(matched[:i])
	quorum := i/2 + 1
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
		l.notifyFlr(false) // we updated commit index
	}
}

// if commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
func (l *ldrShip) applyCommitted() {
	// add all entries <=commitIndex & add only non-log entries at commitIndex+1
	var prev, ne *newEntry = nil, l.neHead
	for ne != nil {
		if ne.index <= l.commitIndex {
			prev, ne = ne, ne.next
		} else if ne.index == l.commitIndex+1 && !ne.isLogEntry() {
			prev, ne = ne, ne.next
		} else {
			break
		}
	}
	var head *newEntry
	if prev != nil {
		head = l.neHead
		prev.next = nil
		l.neHead = ne
		if l.neHead == nil {
			l.neTail = nil
		}
	}

	apply := fsmApply{head, l.log.ViewAt(l.log.PrevIndex(), l.commitIndex)}
	debug(l, apply)
	l.fsm.ch <- apply
}

func (l *ldrShip) notifyFlr(includeConfig bool) {
	update := leaderUpdate{
		log:         l.log.ViewAt(l.removeLTE, l.lastLogIndex),
		commitIndex: l.commitIndex,
	}
	if includeConfig {
		update.config = &l.configs.Latest
	}
	for _, f := range l.flrs {
		select {
		case f.fromLeaderCh <- update:
		case <-f.fromLeaderCh:
			f.fromLeaderCh <- update
		}
		debug(l, update, f.status.id)
	}
	if l.voter {
		l.onMajorityCommit()
	}
}

func (l *ldrShip) checkLogCompact() {
	for _, f := range l.flrs {
		if f.status.removeLTE < l.removeLTE {
			return
		}
	}
	l.compactLog(l.removeLTE)
}
