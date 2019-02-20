package raft

import (
	"sync/atomic"
	"time"
)

type replication struct {
	member           *member
	storage          *storage
	heartbeatTimeout time.Duration

	nextIndex  uint64
	matchIndex uint64

	// leader notifies replication with update
	leaderUpdateCh chan leaderUpdate

	matchUpdatedCh chan<- *replication
	newTermCh      chan<- uint64
	stopCh         <-chan struct{}

	str string // used for debug() calls
}

const maxAppendEntries = 64 // todo: should be configurable

func (repl *replication) runLoop(req *appendEntriesRequest) {
	lastIndex, matchIndex := req.prevLogIndex, repl.getMatchIndex()

	// know which entries to replicate: fixes repl.nextIndex and repl.matchIndex
	// after loop: repl.matchIndex + 1 == repl.nextIndex
	for matchIndex+1 != repl.nextIndex {
		repl.storage.fillEntries(req, repl.nextIndex, repl.nextIndex-1) // zero entries
		resp, stop := repl.retryAppendEntries(req)
		if stop {
			return
		} else if resp.success {
			matchIndex = req.prevLogIndex
			repl.setMatchIndex(matchIndex)
			break
		} else {
			repl.nextIndex = max(min(repl.nextIndex-1, resp.lastLogIndex+1), 1)
		}
		select {
		case <-repl.stopCh:
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
		case <-repl.stopCh:
			return
		case update := <-repl.leaderUpdateCh:
			lastIndex, req.leaderCommitIndex = update.lastIndex, update.commitIndex
			debug(repl, "{last:", lastIndex, "commit:", req.leaderCommitIndex, "} <-leaderUpdateCh")
			timerCh = closedCh
		case <-timerCh:
		}

		// setup request
		if matchIndex < lastIndex {
			// replication of entries [repl.nextIndex, lastIndex] is pending
			maxIndex := min(lastIndex, repl.nextIndex+uint64(maxAppendEntries)-1)
			repl.storage.fillEntries(req, repl.nextIndex, maxIndex)
			debug(repl, ">> appendEntriesRequest", len(req.entries))
		} else {
			// send heartbeat
			req.prevLogIndex, req.prevLogTerm, req.entries = lastIndex, req.term, nil // zero entries
			debug(repl, ">> heartbeat")
		}

		resp, stop := repl.retryAppendEntries(req)
		if stop {
			return
		} else if !resp.success {
			// follower have transitioned to candidate and started election
			assert(resp.term > req.term, "%s follower must have started election", repl)
			return
		}

		repl.nextIndex = resp.lastLogIndex + 1
		matchIndex = resp.lastLogIndex
		repl.setMatchIndex(matchIndex)

		if matchIndex < lastIndex {
			// replication of entries [repl.nextIndex, lastIndex] is still pending: no more sleeping!!!
			timerCh = closedCh
		} else {
			timerCh = afterRandomTimeout(repl.heartbeatTimeout / 10)
		}
	}
}

func (repl *replication) getMatchIndex() uint64 {
	return atomic.LoadUint64(&repl.matchIndex)
}

func (repl *replication) setMatchIndex(v uint64) {
	atomic.StoreUint64(&repl.matchIndex, v)
	select {
	case <-repl.stopCh:
	case repl.matchUpdatedCh <- repl:
	}
}

// retries request until success or got stop signal
// last return value is true in case of stop signal
func (repl *replication) retryAppendEntries(req *appendEntriesRequest) (*appendEntriesResponse, bool) {
	var failures uint64
	for {
		resp, err := repl.appendEntries(req)
		if err != nil {
			failures++
			select {
			case <-repl.stopCh:
				return resp, true
			case <-time.After(backoff(failures)):
				debug(repl, "retry appendEntries")
				continue
			}
		}
		if resp.term > req.term {
			select {
			case <-repl.stopCh:
			case repl.newTermCh <- resp.term:
			}
			return resp, true
		}
		return resp, false
	}
}

func (repl *replication) appendEntries(req *appendEntriesRequest) (*appendEntriesResponse, error) {
	resp := new(appendEntriesResponse)
	err := repl.member.doRPC(rpcAppendEntries, req, resp)
	repl.member.contactSucceeded(err == nil)
	return resp, err
}

func (repl *replication) String() string {
	return repl.str
}
