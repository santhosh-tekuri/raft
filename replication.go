package raft

import (
	"fmt"
	"time"
)

type leaderUpdate struct {
	lastIndex, commitIndex uint64
}

type replication struct {
	// this is owned by ldr goroutine
	status replStatus

	connPool  *connPool
	log       *log
	hbTimeout time.Duration
	conn      *netConn

	// leader notifies replication with update
	ldrUpdateCh chan leaderUpdate

	// replication notifies leader about our progress
	replUpdatedCh chan<- replUpdate
	newTermCh     chan<- uint64
	stopCh        chan struct{}

	str string // used for debug() calls
}

const maxAppendEntries = 64 // todo: should be configurable

func (repl *replication) runLoop(req *appendEntriesRequest) {
	defer func() {
		if repl.conn != nil {
			repl.connPool.returnConn(repl.conn)
		}
	}()

	ldrLastIndex := req.prevLogIndex
	matchIndex, nextIndex := uint64(0), ldrLastIndex+1

	// from this time node is unreachable
	// zero value means node is reachable
	var noContact time.Time

	debug(repl, "repl.start ldrLastIndex:", ldrLastIndex, "matchIndex:", matchIndex, "nextIndex:", nextIndex)

	for {
		// prepare request ----------------------------
		if nextIndex == 1 {
			req.prevLogIndex, req.prevLogTerm = 0, 0
		} else if nextIndex-1 == ldrLastIndex {
			req.prevLogIndex, req.prevLogTerm = ldrLastIndex, req.term
		} else {
			prevEntry := &entry{}
			repl.log.getEntry(nextIndex-1, prevEntry)
			req.prevLogIndex, req.prevLogTerm = prevEntry.index, prevEntry.term
		}
		var n uint64 // number of entries to be sent
		if matchIndex+1 == nextIndex {
			n = min(ldrLastIndex-matchIndex, maxAppendEntries)
		}
		if n == 0 {
			req.entries = nil
		} else {
			req.entries = make([]*entry, n)
			for i := range req.entries {
				req.entries[i] = &entry{}
				repl.log.getEntry(nextIndex+uint64(i), req.entries[i])
			}
		}

		// send request ----------------------------------
		var resp *appendEntriesResponse
		var err error
		var failures uint64
		for {
			resp, err = repl.appendEntries(req)
			if err != nil {
				if noContact.IsZero() {
					noContact = time.Now()
					debug(repl, "noContact", err)
					repl.notifyLdr(matchIndex, noContact)
				}
				failures++
				select {
				case <-repl.stopCh:
					return
				case <-time.After(backOff(failures)):
					continue
				}
			}
			break
		}

		// process response ------------------------------
		if !noContact.IsZero() {
			noContact = time.Time{} // zeroing
			debug(repl, "yesContact")
			repl.notifyLdr(matchIndex, noContact)
		}
		if resp.term > req.term {
			select {
			case <-repl.stopCh:
			case repl.newTermCh <- resp.term:
			}
			return
		}
		if resp.success {
			old := matchIndex
			if len(req.entries) == 0 {
				matchIndex = req.prevLogIndex
			} else {
				matchIndex = resp.lastLogIndex
				nextIndex = matchIndex + 1
			}
			if matchIndex != old {
				debug(repl, "matchIndex:", matchIndex)
				repl.notifyLdr(matchIndex, noContact)
			}
		} else {
			if matchIndex+1 != nextIndex {
				nextIndex = max(min(nextIndex-1, resp.lastLogIndex+1), 1)
				debug(repl, "nextIndex:", nextIndex)
			} else {
				// todo: notify leader, that we stopped and dont panic
				panic(fmt.Sprintf("Raft: faulty follower %s. remove it from cluster", repl.status.id))
			}
		}

		if matchIndex == ldrLastIndex {
			// nothing to replicate. start heartbeat timer
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.ldrUpdateCh:
				ldrLastIndex, req.ldrCommitIndex = update.lastIndex, update.commitIndex
				debug(repl, "{last:", ldrLastIndex, "commit:", req.ldrCommitIndex, "} <-ldrUpdateCh")
			case <-afterRandomTimeout(repl.hbTimeout / 10):
			}
		} else {
			// check signal if any, without blocking
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.ldrUpdateCh:
				ldrLastIndex, req.ldrCommitIndex = update.lastIndex, update.commitIndex
				debug(repl, "{last:", ldrLastIndex, "commit:", req.ldrCommitIndex, "} <-ldrUpdateCh")
			default:
			}
		}
	}
}

func (repl *replication) notifyLdr(matchIndex uint64, noContact time.Time) {
	update := replUpdate{status: &repl.status, matchIndex: matchIndex, noContact: noContact}
	select {
	case <-repl.stopCh:
	case repl.replUpdatedCh <- update:
	}
}

func (repl *replication) appendEntries(req *appendEntriesRequest) (*appendEntriesResponse, error) {
	if repl.conn == nil {
		conn, err := repl.connPool.getConn()
		if err != nil {
			return nil, err
		}
		repl.conn = conn
	}
	resp := new(appendEntriesResponse)
	err := repl.conn.doRPC(rpcAppendEntries, req, resp)
	if err != nil {
		_ = repl.conn.close()
		repl.conn = nil
	}
	return resp, err
}

func (repl *replication) String() string {
	return repl.str
}

// ------------------------------------------------

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

// backOff is used to compute an exponential backOff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor.
func backOff(round uint64) time.Duration {
	base, limit := failureWait, uint64(maxFailureScale)
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	return base
}

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
