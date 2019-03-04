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

	ldrStartIndex uint64
	connPool      *connPool
	storage       *storage
	hbTimeout     time.Duration
	conn          *netConn

	// leader notifies replication with update
	ldrUpdateCh chan leaderUpdate

	// replication notifies leader about our progress
	replUpdatedCh chan<- replUpdate
	newTermCh     chan<- uint64
	stopCh        chan struct{}

	trace *Trace
	str   string // used for debug() calls
}

const maxAppendEntries = 64 // todo: should be configurable

// todo: lots of panics!!!
func (repl *replication) runLoop(req *appendEntriesReq) {
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
		// find prevEntry ----------------------------
		prevIndex, prevTerm, err := nextIndex-1, uint64(0), errNoEntryFound
		if prevIndex == 0 {
			prevTerm, err = 0, nil
		} else if prevIndex < repl.ldrStartIndex {
			prevTerm, err = repl.storage.getEntryTerm(prevIndex)
		} else if repl.storage.hasEntry(prevIndex) {
			prevTerm, err = req.term, nil
		}
		if err == errNoEntryFound {
			prevIndex, prevTerm = repl.getSnapLog()
		} else if err != nil {
			panic(err)
		}

		// prepare request----------------------------
		var reqMsg request
		var respMsg message

		if prevIndex == nextIndex-1 {
			req.prevLogIndex, req.prevLogTerm = prevIndex, prevTerm
			reqMsg, respMsg = req, &appendEntriesResp{}

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
					repl.storage.getEntry(nextIndex+uint64(i), req.entries[i])
				}
			}
		} else {
			debug(repl, "preparing installSnapReq")
			meta, snapshot, err := repl.storage.snapshots.Open(prevIndex)
			if err != nil {
				panic(err)
			}
			installReq := &installSnapReq{
				term:       req.term,
				leader:     req.leader,
				lastIndex:  meta.Index,
				lastTerm:   meta.Term,
				lastConfig: meta.Config,
				size:       meta.Size,
				snapshot:   snapshot,
			}
			reqMsg, respMsg = installReq, &installSnapResp{}
		}

		// send request ----------------------------------
		var failures uint64
		for {
			err = repl.doRPC(reqMsg, respMsg)
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
		if !noContact.IsZero() {
			noContact = time.Time{} // zeroing
			debug(repl, "yesContact")
			repl.notifyLdr(matchIndex, noContact)
		}
		if respMsg.getTerm() > req.term {
			select {
			case <-repl.stopCh:
			case repl.newTermCh <- respMsg.getTerm():
			}
			return
		}

		// process response ------------------------------
		if reqMsg.rpcType() == rpcAppendEntries {
			resp := respMsg.(*appendEntriesResp)
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
					assert(false, fmt.Sprintf("Raft: faulty follower %s. remove it from cluster", repl.status.id))
				}
			}
		} else {
			req, resp := reqMsg.(*installSnapReq), respMsg.(*installSnapResp)
			if resp.success {
				nextIndex = req.lastIndex + 1
				matchIndex = req.lastIndex
				debug(repl, "matchIndex:", matchIndex)
				repl.notifyLdr(matchIndex, noContact)
			} else {
				panic("faulty follower: installSnapshot failed")
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

func (repl *replication) doRPC(req request, resp message) error {
	if repl.conn == nil {
		conn, err := repl.connPool.getConn()
		if err != nil {
			return err
		}
		repl.conn = conn
	}
	if repl.trace.sending != nil {
		repl.trace.sending(req.from(), repl.connPool.id, req)
	}
	err := repl.conn.doRPC(req, resp)
	if err != nil {
		_ = repl.conn.close()
		repl.conn = nil
	}
	if repl.trace.sending != nil && err == nil {
		repl.trace.received(req.from(), repl.connPool.id, resp)
	}
	return err
}

func (repl *replication) getSnapLog() (snapIndex, snapTerm uint64) {
	// snapshoting might be in progress
	repl.storage.snapMu.RLock()
	defer repl.storage.snapMu.RUnlock()
	return repl.storage.snapIndex, repl.storage.snapTerm
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
