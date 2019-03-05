package raft

import (
	"io"
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
	var reqMsg request
	var respMsg message
	var sendEntries bool
mainLoop:
	for {
		// prepare request----------------------------
		debug(repl, "matchIndex", matchIndex, "nextIndex", nextIndex)
		assert(matchIndex < nextIndex, "%s assert %d<%d", repl, matchIndex, nextIndex)
		if repl.storage.hasEntry(nextIndex) {
			req.prevLogIndex = nextIndex - 1

			// fill req.prevLogTerm
			if req.prevLogIndex == 0 {
				req.prevLogTerm = 0
			} else if req.prevLogIndex >= repl.ldrStartIndex { // being smart!!!
				req.prevLogTerm = req.term
			} else if snapIndex, snapTerm := repl.getSnapLog(); req.prevLogIndex == snapIndex {
				req.prevLogTerm = snapTerm
			} else {
				prevTerm, err := repl.storage.getEntryTerm(req.prevLogIndex)
				if err == errNoEntryFound {
					continue mainLoop // leader compacted logs
				} else if err != nil {
					assert(false, err.Error())
				}
				req.prevLogTerm = prevTerm
			}

			reqMsg, respMsg = req, &appendEntriesResp{}

			var n uint64
			if sendEntries {
				assert(matchIndex+1 == nextIndex, "%s assert %d+1!=%d", repl, matchIndex, nextIndex)
				n = min(ldrLastIndex-matchIndex, maxAppendEntries)
			}
			if n > 0 {
				req.entries = make([]*entry, n)
				for i := range req.entries {
					req.entries[i] = &entry{}
					err := repl.storage.getEntry(nextIndex+uint64(i), req.entries[i])
					if err == errNoEntryFound {
						continue mainLoop // leader compacted logs
					} else if err != nil {
						assert(false, err.Error())
					}
				}
				debug(repl, "sending", req)
			} else {
				req.entries = nil
				if sendEntries {
					debug(repl, "sending heartbeat")
				}
			}
		} else {
			debug(repl, nextIndex, "is not in log, preparing installSnapReq")
			reqMsg = &installLatestSnapReq{
				installSnapReq: installSnapReq{
					term:   req.term,
					leader: req.leader,
				},
				snapshots: repl.storage.snapshots,
			}
			respMsg = &installSnapResp{}
		}

		// send request ----------------------------------
		var failures uint64
		for {
			err := repl.doRPC(reqMsg, respMsg)
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
			sendEntries = resp.success
			if resp.success {
				old := matchIndex
				matchIndex, _ = lastEntry(req)
				nextIndex = matchIndex + 1
				if matchIndex != old {
					debug(repl, "matchIndex:", matchIndex)
					repl.notifyLdr(matchIndex, noContact)
				}
			} else {
				nextIndex = min(nextIndex-1, resp.lastLogIndex+1)
				debug(repl, "nextIndex:", nextIndex)
				assert(matchIndex < nextIndex, "%s assert %d<%d => faulty follower", repl, matchIndex, nextIndex)
			}
		} else {
			req, resp := reqMsg.(*installLatestSnapReq), respMsg.(*installSnapResp)
			sendEntries = resp.success
			if resp.success {
				matchIndex = req.lastIndex
				nextIndex = matchIndex + 1
				debug(repl, "matchIndex:", matchIndex)
				repl.notifyLdr(matchIndex, noContact)
			} else {
				assert(false, "%s installSnapshot failed", repl)
			}
		}

		if sendEntries && matchIndex == ldrLastIndex {
			// nothing to send. start heartbeat timer
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

type installLatestSnapReq struct {
	installSnapReq
	snapshots Snapshots
}

func (req *installLatestSnapReq) encode(w io.Writer) error {
	meta, snapshot, err := req.snapshots.Open()
	if err != nil {
		panic(err)
	}
	req.lastIndex = meta.Index
	req.lastTerm = meta.Term
	req.lastConfig = meta.Config
	req.size = meta.Size
	req.snapshot = snapshot
	return req.installSnapReq.encode(w)
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
