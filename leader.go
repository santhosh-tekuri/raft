// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"sort"
	"sync"
	"time"
)

type leader struct {
	*Raft

	node      Node
	numVoters int

	// leader term starts from this index.
	// this index refers to noop entry
	startIndex uint64

	// queue in which user submitted entries are enqueued
	// committed entries are dequeued and handed over to fsm go-routine
	neHead, neTail *newEntry

	// holds running replications, key is addr
	repls map[uint64]*replication
	wg    sync.WaitGroup

	// to receive updates from replicators
	replUpdateCh chan replUpdate

	transfer   transfer
	waitStable []waitForStableConfig

	removeLTE uint64
}

func (l *leader) init() {
	assert(l.leader == l.nid)
	l.node = l.configs.Latest.Nodes[l.nid]
	l.numVoters = l.configs.Latest.numVoters()
	l.startIndex = l.lastLogIndex + 1
	l.replUpdateCh = make(chan replUpdate, 1024)
	l.removeLTE = l.log.PrevIndex()

	// start replication routine for each follower
	for id, n := range l.configs.Latest.Nodes {
		if id != l.nid {
			l.addReplication(n)
		}
	}
	l.checkConfigActions(nil, l.configs.Latest)

	// add a blank no-op entry into log at the start of its term
	l.storeEntry(&newEntry{entry: &entry{typ: entryNop}})
}

func (l *leader) onTimeout() { l.checkQuorum(0) }

func (l *leader) release() {
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

	if trace {
		println(l, "stopping followers")
	}
	for id, repl := range l.repls {
		close(repl.stopCh)
		delete(l.repls, id)
	}
	if l.leader == l.nid {
		l.setLeader(0)
	}

	// respond to any pending user entries
	var err error = notLeaderError(l.Raft, true)
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
	l.replUpdateCh = nil
}

func (l *leader) storeEntry(ne *newEntry) {
	i, lastIndex, configIndex := 0, l.lastLogIndex, l.configs.Latest.Index
	for {
		i++
		if l.transfer.inProgress() {
			ne.reply(InProgressError("transferLeadership"))
		} else if !l.node.Voter {
			if _, ok := l.configs.Latest.Nodes[l.nid]; ok {
				ne.reply(InProgressError("demoteLeader"))
			} else {
				ne.reply(InProgressError("removeLeader"))
			}
		} else {
			ne.entry.index, ne.entry.term = l.lastLogIndex+1, l.term
			if l.neTail != nil {
				l.neTail.next, l.neTail = ne, ne
			} else {
				l.neHead, l.neTail = ne, ne
			}
			if ne.isLogEntry() {
				if trace {
					println(l, "log.append", ne.typ, ne.index)
				}
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
	if trace {
		println(l, "got batch of", i, "entries")
	}
	if l.neHead != nil && !l.neHead.isLogEntry() {
		l.applyCommitted()
	}
	if l.lastLogIndex > lastIndex {
		l.beginFinishedRounds()
		l.notifyFlr(l.configs.Latest.Index > configIndex)
		if l.numVoters == 1 && l.node.Voter {
			l.onMajorityCommit()
		}
	}
}

func (l *leader) addReplication(n Node) {
	assert(n.ID != l.nid) // no replication for leader
	repl := &replication{
		node:           n,
		rtime:          newRandTime(),
		status:         replicationStatus{id: n.ID, node: n, removeLTE: l.removeLTE},
		ldrStartIndex:  l.startIndex,
		ldrLastIndex:   l.lastLogIndex,
		matchIndex:     0,
		nextIndex:      l.lastLogIndex + 1,
		connPool:       l.getConnPool(n.ID),
		hbTimeout:      l.hbTimeout,
		timer:          newSafeTimer(),
		bandwidth:      l.bandwidth,
		log:            l.storage.log.ViewAt(l.removeLTE, l.lastLogIndex),
		snaps:          l.storage.snaps,
		stopCh:         make(chan struct{}),
		replUpdateCh:   l.replUpdateCh,
		leaderUpdateCh: make(chan leaderUpdate, 1),
	}
	l.repls[n.ID] = repl

	// send initial empty AppendEntries RPCs (heartbeat) to each follower
	req := &appendReq{
		req:            req{l.term, l.nid},
		ldrCommitIndex: l.commitIndex,
		prevLogIndex:   l.lastLogIndex,
		prevLogTerm:    l.lastLogTerm,
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		repl.runLoop(req)
		if trace {
			println(repl, "repl.End")
		}
	}()
}

func (l *leader) checkReplUpdates(u replUpdate) {
	matchUpdated, noContactUpdated, removeLTEUpdated := false, false, false
	for {
		if trace {
			println(l, "<<", u)
		}
		status := u.status
		if !status.removed {
			switch u := u.update.(type) {
			case error:
				panic(u)
			case matchIndex:
				matchUpdated = true
				status.matchIndex = u.val
				if !status.node.Voter && status.node.Action != None {
					// matchIndex update required only for remove and promote
					l.checkConfigAction(nil, l.configs.Latest, status)
				}
			case removeLTE:
				removeLTEUpdated = true
				status.removeLTE = u.val
			case noContact:
				noContactUpdated = true
				status.noContact, status.err = u.time, u.err
				if u.time.IsZero() {
					l.logger.Info("node", status.id, "is reachable now")
				} else {
					l.logger.Warn("node", status.id, "is unreachable, reason:", u.err)
				}
				if l.trace.Unreachable != nil {
					l.trace.Unreachable(l.liveInfo(), status.id, u.time, u.err)
				}
			case newTerm:
				// if response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				l.setState(Follower)
				l.setLeader(0)
				l.setTerm(u.val)
				return
			}
		}
		// get any waiting update
		select {
		case <-l.close:
			return
		case u = <-l.replUpdateCh:
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

func (l *leader) checkQuorum(wait time.Duration) {
	voters, reachable := 0, 0
	for id, n := range l.configs.Latest.Nodes {
		if n.Voter {
			voters++
			if id == l.nid || l.repls[id].status.noContact.IsZero() {
				reachable++
			}
		}
	}

	if reachable >= voters/2+1 {
		if l.timer.active {
			if trace {
				println(l, "quorumReachable")
			}
			l.logger.Info("quorum is reachable now")
			if l.trace.QuorumUnreachable != nil {
				l.trace.QuorumUnreachable(l.liveInfo(), time.Time{})
			}
			l.timer.stop()
		}
		return
	}

	if l.quorumWait == 0 || !l.timer.active {
		l.logger.Info("quorum is unreachable")
		if l.trace.QuorumUnreachable != nil {
			l.trace.QuorumUnreachable(l.liveInfo(), time.Now())
		}
	}
	if wait == 0 {
		if trace {
			println(l, "quorumUnreachable: stepping down")
		}
		l.setState(Follower)
		l.setLeader(0)
	} else if !l.timer.active {
		if trace {
			println(l, "quorumUnreachable: waiting", wait)
		}
		l.timer.reset(wait)
	}
}

// computes N such that, a majority of matchIndex[i] ≥ N
func (l *leader) majorityMatchIndex() uint64 {
	if l.numVoters == 1 && l.node.Voter {
		return l.lastLogIndex
	}
	matched := make(decrUint64Slice, len(l.configs.Latest.Nodes))
	i := 0
	for _, n := range l.configs.Latest.Nodes {
		if n.Voter {
			if n.ID == l.nid {
				matched[i] = l.lastLogIndex
			} else {
				matched[i] = l.repls[n.ID].status.matchIndex
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
func (l *leader) onMajorityCommit() {
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
func (l *leader) applyCommitted() {
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
	if trace {
		println(l, apply)
	}
	l.fsm.ch <- apply
}

func (l *leader) notifyFlr(includeConfig bool) {
	update := leaderUpdate{
		log:         l.log.ViewAt(l.removeLTE, l.lastLogIndex),
		commitIndex: l.commitIndex,
	}
	if includeConfig {
		update.config = &l.configs.Latest
	}
	for _, repl := range l.repls {
		select {
		case repl.leaderUpdateCh <- update:
		case <-repl.leaderUpdateCh:
			repl.leaderUpdateCh <- update
		}
		if trace {
			println(l, update, repl.status.id)
		}
	}
}

func (l *leader) checkLogCompact() {
	for _, repl := range l.repls {
		if repl.status.removeLTE < l.removeLTE {
			return
		}
	}
	_ = l.compactLog(l.removeLTE)
}
