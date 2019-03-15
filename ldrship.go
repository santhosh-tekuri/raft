package raft

import (
	"container/list"
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
	newEntries *list.List

	// holds running replications, key is addr
	flrs map[uint64]*flr
	wg   sync.WaitGroup

	// to receive updates from replicators
	fromReplsCh chan interface{}

	transferTimer *safeTimer // is active when transfer is in progress
	transferLdr   transferLdr
}

func (l *ldrShip) init() {
	assert(l.leader == l.id, "%v ldr.leader: got %d, want %d", l, l.leader, l.id)

	l.voter = true
	l.startIndex = l.lastLogIndex + 1
	l.fromReplsCh = make(chan interface{}, len(l.configs.Latest.Nodes))

	// start replication routine for each follower
	for id, node := range l.configs.Latest.Nodes {
		if id != l.id {
			l.addFlr(node)
		}
	}

	// add a blank no-op entry into log at the start of its term
	_ = l.storeEntry(NewEntry{entry: &entry{typ: entryNop}})
}

func (l *ldrShip) onTimeout() { l.checkQuorum(0) }

func (l *ldrShip) release() {
	if l.transferTimer.active {
		var err error
		if l.term > l.transferLdr.term {
			err = nil
		} else if l.isClosing() {
			err = ErrServerClosed
		} else {
			err = ErrQuorumUnreachable
		}
		l.replyTransferLeadership(err)
	}

	for id, f := range l.flrs {
		close(f.stopCh)
		delete(l.flrs, id)
	}
	if l.leader == l.id {
		l.setLeader(0)
	}

	// respond to any pending user entries
	var err error = NotLeaderError{l.leaderAddr(), true, l.appendErr}
	if l.isClosing() {
		err = ErrServerClosed
	}
	for l.newEntries.Len() > 0 {
		ne := l.newEntries.Remove(l.newEntries.Front()).(NewEntry)
		ne.reply(err)
	}

	// wait for replicators to finish
	l.wg.Wait()
	l.fromReplsCh = nil
}

func (l *ldrShip) storeEntry(ne NewEntry) error {
	ne.entry.index, ne.entry.term = l.lastLogIndex+1, l.term
	elem := l.newEntries.PushBack(ne)

	if ne.typ == entryRead || ne.typ == entryBarrier { // non-log entry
		l.applyCommitted()
		return nil
	}

	if l.transferTimer.active {
		l.newEntries.Remove(elem)
		ne.reply(InProgressError("transferLeadership"))
		return InProgressError("transferLeadership")
	}

	debug(l, "log.append", ne.typ, ne.index)
	if err := l.storage.appendEntry(ne.entry); err != nil {
		if l.trace.Error != nil {
			l.trace.Error(err)
		}
		// ne is replied in release
		l.appendErr = err // to prevent startElection
		l.setState(Follower)
		l.setLeader(0)
		return err
	}
	if ne.typ == entryConfig {
		config := Config{}
		err := config.decode(ne.entry)
		assert(err == nil, "BUG: %v", err)
		l.voter = config.isVoter(l.id)
		l.Raft.changeConfig(config)
	}
	l.notifyFlr(ne.typ == entryConfig)
	return nil
}

func (l *ldrShip) addFlr(node Node) {
	assert(node.ID != l.id, "adding leader as follower")
	f := &flr{
		node:          node,
		rtime:         newRandTime(),
		status:        flrStatus{id: node.ID},
		ldrStartIndex: l.startIndex,
		ldrLastIndex:  l.lastLogIndex,
		matchIndex:    0,
		nextIndex:     l.lastLogIndex + 1,
		connPool:      l.getConnPool(node.ID),
		hbTimeout:     l.hbTimeout,
		storage:       l.storage,
		stopCh:        make(chan struct{}),
		toLeaderCh:    l.fromReplsCh,
		fromLeaderCh:  make(chan leaderUpdate, 1),
		trace:         &l.trace,
		str:           fmt.Sprintf("%v M%d", l, node.ID),
	}
	l.flrs[node.ID] = f

	// send initial empty AppendEntries RPCs (heartbeat) to each follower
	req := &appendEntriesReq{
		term:           l.term,
		leader:         l.id,
		ldrCommitIndex: l.commitIndex,
		prevLogIndex:   l.lastLogIndex,
		prevLogTerm:    l.lastLogTerm,
	}

	// don't retry on failure. so that we can respond to apply/inspect
	debug(f, ">> firstHeartbeat")
	_ = f.doRPC(req, &appendEntriesResp{})

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		f.replicate(req)
		debug(f, "f.replicateEnd")
	}()
}

func (l *ldrShip) checkReplUpdates(u interface{}) {
	matchUpdated, noContactUpdated := false, false
	for {
		switch u := u.(type) {
		case matchIndex:
			matchUpdated = true
			u.status.matchIndex = u.val
		case noContact:
			noContactUpdated = true
			u.status.noContact = u.time
			if l.trace.Unreachable != nil {
				l.trace.Unreachable(l.liveInfo(), u.status.id, u.time, u.err)
			}
		case newTerm:
			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			debug(l, "leader -> follower")
			l.setState(Follower)
			l.setLeader(0)
			l.setTerm(u.val)
			return
		case roundCompleted:
			r := u.round
			if r.Ordinal > u.status.rounds {
				debug(l, "completed", r)
				u.status.rounds++
				if l.trace.RoundCompleted != nil {
					l.trace.RoundCompleted(l.liveInfo(), u.status.id, r)
				}
			} else {
				debug(l, u.status.id, "is reminding promotion:", r)
			}
			if l.transferTimer.active {
				debug(l, "cannot promote: transferLeadership in progress")
				break
			}
			if !l.configs.IsCommitted() {
				debug(l, "cannot promote: config not committed")
				break
			}
			hasNewEntries := l.lastLogIndex > u.status.matchIndex
			if hasNewEntries && r.Duration() > l.promoteThreshold {
				debug(l, "best of luck for next round")
				break // best of luck for next round !!!
			}
			n, ok := l.configs.Latest.Nodes[u.status.id]
			if !ok || !n.promote() {
				debug(l, "this node should not be promoted")
				break
			}

			// promoting flr
			debug(l, "promoting", n.ID)
			config := l.configs.Latest.clone()
			n.Voter, n.Promote = true, false
			config.Nodes[n.ID] = n
			if l.trace.Promoting != nil {
				l.trace.Promoting(l.liveInfo(), n.ID, r.Ordinal)
			}
			l.doChangeConfig(nil, config)
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
	if matchUpdated || noContactUpdated {
		if l.transferTimer.active && l.transferLdr.rpcCh == nil {
			if tgt := l.choseTransferTgt(); tgt != 0 {
				l.doTransferLeadership(tgt)
			}
		}
	}
}

func (l *ldrShip) checkQuorum(wait time.Duration) {
	voters, reachable := 0, 0
	for id, n := range l.configs.Latest.Nodes {
		if n.Voter {
			voters++
			if id == l.id || l.flrs[id].status.noContact.IsZero() {
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

	// todo: if quorum unreachable raise alert
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
			if n.ID == l.id {
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
	for {
		// send query/barrier entries if any to fsm
		for l.newEntries.Len() > 0 {
			elem := l.newEntries.Front()
			ne := elem.Value.(NewEntry)
			if ne.index == l.lastApplied+1 && (ne.typ == entryRead || ne.typ == entryBarrier) {
				l.newEntries.Remove(elem)
				debug(l, "fms <- {", ne.typ, ne.index, "}")
				select {
				case <-l.close:
					ne.reply(ErrServerClosed)
					return
				case l.fsm.taskCh <- ne:
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

func (l *ldrShip) notifyFlr(includeConfig bool) {
	update := leaderUpdate{
		lastIndex:   l.lastLogIndex,
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
	}
	if l.voter {
		l.onMajorityCommit()
	}
}

// todo: if transferTgt is unreachable, try another another transferTgt
func (l *ldrShip) onTransferLeadership(t transferLdr) {
	debug(l, "got", t)
	if err := l.validateTransferLeadership(t); err != nil {
		debug(l, "transferLdr rejected:", err)
		t.reply(err)
		return
	}

	t.term = l.term
	l.transferLdr = t
	l.transferTimer.reset(t.timeout)
	if tgt := l.choseTransferTgt(); tgt != 0 {
		l.doTransferLeadership(tgt)
	}
}

func (l *ldrShip) validateTransferLeadership(t transferLdr) error {
	if l.transferTimer.active {
		return InProgressError("transferLeadership")
	}
	if l.configs.Latest.numVoters() == 1 {
		return ErrLeadershipTransferNoVoter
	}
	if t.target != 0 {
		if t.target == l.id {
			return ErrLeadershipTransferSelf
		}
		if n, ok := l.configs.Latest.Nodes[t.target]; ok {
			if !n.Voter {
				return ErrLeadershipTransferTargetNonvoter
			}
		} else {
			return ErrLeadershipTransferInvalidTarget
		}
	}
	return nil
}

func (l *ldrShip) choseTransferTgt() uint64 {
	if l.transferLdr.target != 0 {
		f := l.flrs[l.transferLdr.target]
		if f.status.noContact.IsZero() && f.status.matchIndex == l.lastLogIndex {
			return l.transferLdr.target
		}
	} else {
		for id, n := range l.configs.Latest.Nodes {
			if id != l.id && n.Voter {
				f := l.flrs[id]
				if f.status.noContact.IsZero() && f.status.matchIndex == l.lastLogIndex {
					return id
				}
			}
		}
	}
	return 0
}

func (l *ldrShip) doTransferLeadership(target uint64) {
	debug(l, "transferring leadership:", target)
	pool := l.getConnPool(target)
	ch := make(chan timeoutNowResult, 1)
	l.transferLdr.rpcCh = ch
	req := &timeoutNowReq{l.term, l.id}
	go func() {
		conn, err := pool.getConn()
		if err != nil {
			ch <- timeoutNowResult{target: pool.id, err: err}
			return
		}
		resp := new(timeoutNowResp)
		debug(l.id, ">>", req)
		if l.trace.sending != nil {
			l.trace.sending(l.id, pool.id, Leader, req)
		}
		_ = conn.conn.SetDeadline(time.Now().Add(5 * l.hbTimeout)) // todo
		if err = conn.doRPC(req, resp); err != nil {
			_ = conn.close()
			ch <- timeoutNowResult{target: pool.id, err: err}
			return
		}
		pool.returnConn(conn)
		debug(l.id, "<<", resp)
		if l.trace.received != nil {
			l.trace.received(l.id, pool.id, Leader, req.term, resp)
		}
		ch <- timeoutNowResult{target: pool.id, err: nil}
	}()
}

func (l *ldrShip) onTransferTimeout() {
	l.replyTransferLeadership(TimeoutError("transferLeadership"))
}

func (l *ldrShip) onTimeoutNowResult(result timeoutNowResult) {
	l.transferLdr.rpcCh = nil
	if result.err != nil {
		f := l.flrs[result.target]
		if f.status.noContact.IsZero() {
			f.status.noContact = time.Now()
		}
		if l.transferLdr.target == 0 {
			if tgt := l.choseTransferTgt(); tgt != 0 {
				l.doTransferLeadership(tgt)
			}
		}
	}
}

func (l *ldrShip) replyTransferLeadership(err error) {
	if l.transferTimer.active {
		debug(l, "reply leadership transfer:", err)
		l.transferLdr.reply(err)
	}
	l.transferTimer.stop()
	l.transferLdr.rpcCh = nil
}
