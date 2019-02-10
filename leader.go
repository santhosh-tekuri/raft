package raft

import (
	"container/list"
	"sort"
)

func (r *Raft) runLeader() {
	termStartIndex := r.lastLogIndex + 1

	newEntries := list.New()

	// add a blank no-op entry into log at the start of its term
	r.storeNewEntry(newEntries, newEntry{
		entry: &entry{
			typ: entryNoop,
		},
	})

	recalculateMatchCh := make(chan *member, 2*len(r.members)) // room given for any new members

	// to send stop signal to replicators
	stopCh := make(chan struct{})
	defer close(stopCh)

	// to recieve stale term notification from replicators
	stepDownCh := make(chan command, len(r.members))

	// start replication routine for each follower
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}

		// follower's nextIndex initialized to leader last log index + 1
		m.nextIndex = r.lastLogIndex + 1

		// send initial empty AppendEntries RPCs (heartbeat) to each follower
		req := &appendEntriesRequest{
			term:              r.term,
			leaderID:          r.addr,
			leaderCommitIndex: r.commitIndex,
			prevLogIndex:      r.lastLogIndex,
			prevLogTerm:       r.lastLogTerm,
		}
		debug(r, "heartbeat ->", m.addr)
		m.retryAppendEntries(req, stopCh, stepDownCh)

		r.wg.Add(1)
		go m.replicate(r.storage, req, recalculateMatchCh, stopCh, stepDownCh)
	}

	for r.state == leader {
		select {
		case cmd := <-stepDownCh:
			r.checkTerm(cmd)

		case rpc := <-r.server.rpcCh:
			r.processRPC(rpc)

		case m := <-recalculateMatchCh:
			// get latest matchIndex from all notified members
			m.matchedIndex = m.getMatchIndex()
		loop:
			for {
				select {
				case m := <-recalculateMatchCh:
					m.matchedIndex = m.getMatchIndex()
				default:
					break loop
				}
			}

			r.recalculateMatch(termStartIndex)
			r.fsmApply(newEntries)

		case newEntry := <-r.applyCh:
			r.storeNewEntry(newEntries, newEntry)
		case f := <-r.inspectCh:
			f(r)
		}
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (r *Raft) recalculateMatch(termStartIndex uint64) {
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
	majorityMatchIndex := matched[r.quorumSize()-1]
	if majorityMatchIndex > r.commitIndex && majorityMatchIndex >= termStartIndex {
		r.commitIndex = majorityMatchIndex
		r.notifyReplicators()
	}
}

func (r *Raft) storeNewEntry(newEntries *list.List, newEntry newEntry) {
	newEntry.index, newEntry.term = r.lastLogIndex+1, r.term

	// append entry to local log
	if newEntry.typ == entryNoop {
		debug(r, "log.append noop", newEntry.index)
	} else {
		debug(r, "log.append cmd", newEntry.index)
	}
	r.storage.append(newEntry.entry)
	r.lastLogIndex++
	r.notifyReplicators()

	newEntries.PushBack(newEntry)
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
