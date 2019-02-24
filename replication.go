package raft

import (
	"time"
)

type leaderUpdate struct {
	lastIndex, commitIndex uint64
}

type replication struct {
	// this is owned by ldr goroutine
	status replStatus

	connPool         *connPool
	storage          *storage
	heartbeatTimeout time.Duration
	conn             *netConn

	// leader notifies replication with update
	leaderUpdateCh chan leaderUpdate

	matchUpdatedCh chan<- replUpdate
	newTermCh      chan<- uint64
	stopCh         chan struct{}

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
		var n uint64 // number of entries to be sent
		if matchIndex+1 == nextIndex {
			n = ldrLastIndex - matchIndex // number of entries to be sent
			if n > maxAppendEntries {
				n = maxAppendEntries
			}
		}
		if nextIndex == 1 {
			req.prevLogIndex, req.prevLogTerm = 0, 0
		} else if nextIndex-1 == ldrLastIndex {
			req.prevLogIndex, req.prevLogTerm = ldrLastIndex, req.term
		} else {
			prevEntry := &entry{}
			repl.storage.getEntry(nextIndex-1, prevEntry)
			req.prevLogIndex, req.prevLogTerm = prevEntry.index, prevEntry.term
		}

		if n == 0 {
			req.entries = nil
		} else {
			req.entries = make([]*entry, n)
			for i := range req.entries {
				req.entries[i] = &entry{}
				repl.storage.getEntry(nextIndex+uint64(i), req.entries[i])
			}
		}

		// send request ----------------------------------
		var failures uint64
		for {
			resp, err := repl.appendEntries(req)
			if err != nil {
				if noContact.IsZero() {
					noContact = time.Now()
					debug(repl, "noContact")
					repl.notifyLdr(matchIndex, noContact)
				}
				failures++
				select {
				case <-repl.stopCh:
					return
				case <-time.After(backoff(failures)):
					continue
				}
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
					nextIndex = resp.lastLogIndex + 1
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
					panic("faulty raft node") // todo: notify leader, that we stopped and dont panic
				}
			}
			break
		}

		if matchIndex == ldrLastIndex {
			// nothing to replicate. start heartbeat timer
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.leaderUpdateCh:
				ldrLastIndex, req.leaderCommitIndex = update.lastIndex, update.commitIndex
				debug(repl, "{last:", ldrLastIndex, "commit:", req.leaderCommitIndex, "} <-leaderUpdateCh")
			case <-afterRandomTimeout(repl.heartbeatTimeout / 10):
			}
		} else {
			// check signal if any, without blocking
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.leaderUpdateCh:
				ldrLastIndex, req.leaderCommitIndex = update.lastIndex, update.commitIndex
				debug(repl, "{last:", ldrLastIndex, "commit:", req.leaderCommitIndex, "} <-leaderUpdateCh")
			default:
			}
		}
	}
}

func (repl *replication) notifyLdr(matchIndex uint64, noContact time.Time) {
	update := replUpdate{status: &repl.status, matchIndex: matchIndex, noContact: noContact}
	select {
	case <-repl.stopCh:
	case repl.matchUpdatedCh <- update:
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
