package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// todo: rename to replication
// todo: implement String() for debug statements
type replicator struct {
	member           *member
	storage          *storage
	heartbeatTimeout time.Duration

	nextIndex  uint64
	matchIndex uint64

	// owned exclusively by raft main goroutine
	// used to recalculateMatch
	matchedIndex uint64

	// leader notifies replicator with update
	leaderUpdateCh chan leaderUpdate

	matchUpdatedCh chan<- *replicator
	newTermCh      chan<- uint64
	stopCh         <-chan struct{}

	// from what time the replicator unable to reach this member
	// zero value means it is reachable
	noContactMu sync.RWMutex
	noContact   time.Time

	ldr string // used for debug() calls from replicator
}

const maxAppendEntries = 64 // todo: should be configurable

func (r *replicator) runLoop(req *appendEntriesRequest) {
	lastIndex, matchIndex := req.prevLogIndex, r.getMatchIndex()

	// know which entries to replicate: fixes r.nextIndex and r.matchIndex
	// after loop: r.matchIndex + 1 == r.nextIndex
	for matchIndex+1 != r.nextIndex {
		r.storage.fillEntries(req, r.nextIndex, r.nextIndex-1) // zero entries
		resp, stop := r.retryAppendEntries(req)
		if stop {
			return
		} else if resp.success {
			matchIndex = req.prevLogIndex
			r.setMatchIndex(matchIndex)
			break
		} else {
			r.nextIndex = max(min(r.nextIndex-1, resp.lastLogIndex+1), 1)
		}
		select {
		case <-r.stopCh:
			return
		default:
		}
	}

	closedCh := func() <-chan time.Time {
		ch := make(chan time.Time)
		close(ch)
		return ch
	}()
	timerCh := closedCh

	for {
		select {
		case <-r.stopCh:
			return
		case update := <-r.leaderUpdateCh:
			lastIndex, req.leaderCommitIndex = update.lastIndex, update.commitIndex
			debug(r.ldr, r.member.addr, "{last:", lastIndex, "commit:", req.leaderCommitIndex, "} <-leaderUpdateCh")
			timerCh = closedCh
		case <-timerCh:
		}

		// setup request
		if matchIndex < lastIndex {
			// replication of entries [r.nextIndex, lastIndex] is pending
			maxIndex := min(lastIndex, r.nextIndex+uint64(maxAppendEntries)-1)
			r.storage.fillEntries(req, r.nextIndex, maxIndex)
			debug(r.ldr, r.member.addr, ">> appendEntriesRequest", len(req.entries))
		} else {
			// send heartbeat
			req.prevLogIndex, req.prevLogTerm, req.entries = lastIndex, req.term, nil // zero entries
			debug(r.ldr, r.member.addr, ">> heartbeat")
		}

		resp, stop := r.retryAppendEntries(req)
		if stop {
			return
		} else if !resp.success {
			// follower have transitioned to candidate and started election
			assert(resp.term > req.term, "%s %s follower must have started election", r.ldr, r.member.addr)
			return
		}

		r.nextIndex = resp.lastLogIndex + 1
		matchIndex = resp.lastLogIndex
		r.setMatchIndex(matchIndex)

		if matchIndex < lastIndex {
			// replication of entries [r.nextIndex, lastIndex] is still pending: no more sleeping!!!
			timerCh = closedCh
		} else {
			timerCh = afterRandomTimeout(r.heartbeatTimeout / 10)
		}
	}
}

func (r *replicator) getMatchIndex() uint64 {
	return atomic.LoadUint64(&r.matchIndex)
}

func (r *replicator) setMatchIndex(v uint64) {
	atomic.StoreUint64(&r.matchIndex, v)
	select {
	case <-r.stopCh:
	case r.matchUpdatedCh <- r:
	}
}

// retries request until success or got stop signal
// last return value is true in case of stop signal
func (r *replicator) retryAppendEntries(req *appendEntriesRequest) (*appendEntriesResponse, bool) {
	var failures uint64
	for {
		resp, err := r.appendEntries(req)
		if err != nil {
			failures++
			select {
			case <-r.stopCh:
				return resp, true
			case <-time.After(backoff(failures)):
				debug(r.ldr, r.member.addr, "retry appendEntries")
				continue
			}
		}
		if resp.term > req.term {
			select {
			case <-r.stopCh:
			case r.newTermCh <- resp.term:
			}
			return resp, true
		}
		return resp, false
	}
}

func (r *replicator) appendEntries(req *appendEntriesRequest) (*appendEntriesResponse, error) {
	resp := new(appendEntriesResponse)
	err := r.member.doRPC(rpcAppendEntries, req, resp)

	r.noContactMu.Lock()
	if err == nil {
		r.noContact = time.Time{} // zeroing
	} else if r.noContact.IsZero() {
		r.noContact = time.Now()
	}
	r.noContactMu.Unlock()

	return resp, err
}
