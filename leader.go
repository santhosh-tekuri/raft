package raft

import (
	"container/list"
	"fmt"
	"sort"
)

func (r *Raft) runLeader() {
	heartbeat := &appendEntriesRequest{
		term:     r.term,
		leaderID: r.addr,
	}

	r.notifyLastLogIndexCh()
	r.notifyCommitIndexCh()

	recalculateMatchCh := make(chan *member, 2*len(r.members)) // room given for any new members

	// start replication routine for each follower
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}

		// follower's nextIndex initialized to leader last log index + 1
		m.nextIndex = r.lastLogIndex + 1

		stopCh := make(chan struct{})
		defer close(stopCh)
		go m.replicate(r.storage, heartbeat, recalculateMatchCh, stopCh)
	}

	newEntries := list.New()

	// add a blank no-op entry into log at the start of its term
	r.processNewEntry(newEntries, newEntry{
		entry: &entry{
			typ: entryNoop,
		},
	})

	// note: we already commited one entry in current term just above
	termStartIndex := r.lastLogIndex

	for r.state == leader {
		select {
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

			for idx := r.recalculateMatch(termStartIndex); r.commitIndex < idx; {
				entry := &entry{}
				r.storage.getEntry(r.commitIndex+1, entry)
				var resp interface{}
				debug(r, "applying cmd", entry.index)
				if entry.typ != entryNoop {
					resp = r.fsm.Apply(entry.data)
				}
				r.commitIndex++
				r.notifyCommitIndexCh()

				elem := newEntries.Front()
				newEntry := elem.Value.(newEntry)
				if newEntry.index == entry.index {
					newEntry.sendResponse(resp)
					newEntries.Remove(elem)
				} else {
					panic(fmt.Sprintf("[BUG] got entry %d, want %d", newEntry.index, entry.index))
				}
			}

		case newEntry := <-r.applyCh:
			r.processNewEntry(newEntries, newEntry)
		case f := <-r.inspectCh:
			f(r)
		}
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (r *Raft) recalculateMatch(termStartIndex uint64) uint64 {
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
		return majorityMatchIndex
	}
	return r.commitIndex
}

func (r *Raft) processNewEntry(newEntries *list.List, newEntry newEntry) {
	entry := newEntry.entry
	newEntry.entry = nil // favor GC

	newEntry.index, entry.index = r.lastLogIndex+1, r.lastLogIndex+1
	entry.term = r.term

	// append entry to local log
	debug(r, "rcvd newentry for index", newEntry.index)
	r.storage.append(entry)
	r.lastLogIndex++
	r.notifyLastLogIndexCh()

	newEntries.PushBack(newEntry)
}

// notify replicators about change to lastLogIndex
func (r *Raft) notifyLastLogIndexCh() {
	for _, m := range r.members {
		if m.addr != r.addr {
			select {
			case m.leaderLastIndexCh <- r.lastLogIndex:
			case <-m.leaderLastIndexCh:
				m.leaderLastIndexCh <- r.lastLogIndex
			}
		}
	}
}

// notify replicators about change to commitIndex
func (r *Raft) notifyCommitIndexCh() {
	for _, m := range r.members {
		if m.addr != r.addr {
			select {
			case m.leaderCommitIndexCh <- r.commitIndex:
			case <-m.leaderCommitIndexCh:
				m.leaderCommitIndexCh <- r.commitIndex
			}
		}
	}
}

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
