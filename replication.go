package raft

import (
	"errors"
	"io"
	"time"
)

type replication struct {
	rtime randTime

	// this is owned by ldr goroutine
	status replStatus

	connPool  *connPool
	storage   *storage
	hbTimeout time.Duration
	conn      *netConn

	ldrStartIndex uint64
	ldrLastIndex  uint64
	matchIndex    uint64
	nextIndex     uint64
	sendEntries   bool

	// from this time node is unreachable
	// zero value means node is reachable
	noContact time.Time

	// leader notifies replication with update
	fromLeaderCh chan leaderUpdate

	// replication notifies leader about our progress
	toLeaderCh chan<- interface{}
	stopCh     chan struct{}

	trace *Trace
	str   string // used for debug() calls
}

func (repl *replication) runLoop(req *appendEntriesReq) {
	defer func() {
		if repl.conn != nil {
			repl.connPool.returnConn(repl.conn)
		}
	}()

	repl.ldrLastIndex = req.prevLogIndex
	repl.matchIndex, repl.nextIndex = uint64(0), repl.ldrLastIndex+1

	debug(repl, "repl.start")
	for {
		debug(repl, "matchIndex", repl.matchIndex, "nextIndex", repl.nextIndex)
		assert(repl.matchIndex < repl.nextIndex, "%s assert %d<%d", repl, repl.matchIndex, repl.nextIndex)

		err := repl.sendAppEntriesReq(req)
		if err == errNoEntryFound {
			err = repl.sendInstallSnapReq(req)
		}

		if err == errStop {
			return
		} else if err != nil {
			if repl.trace.Error != nil {
				repl.trace.Error(err)
			}
			assert(false, "unexpected error: %v", err) // todo
			continue
		}

		if repl.sendEntries && repl.matchIndex == repl.ldrLastIndex {
			// nothing to send. start heartbeat timer
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.fromLeaderCh:
				repl.ldrLastIndex, req.ldrCommitIndex = update.lastIndex, update.commitIndex
				debug(repl, "{last:", repl.ldrLastIndex, "commit:", req.ldrCommitIndex, "} <-fromLeaderCh")
			case <-repl.rtime.after(repl.hbTimeout / 10):
			}
		} else {
			// check signal if any, without blocking
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.fromLeaderCh:
				repl.ldrLastIndex, req.ldrCommitIndex = update.lastIndex, update.commitIndex
				debug(repl, "{last:", repl.ldrLastIndex, "commit:", req.ldrCommitIndex, "} <-fromLeaderCh")
			default:
			}
		}
	}
}

var errStop = errors.New("got stop signal")

func (repl *replication) sendAppEntriesReq(req *appendEntriesReq) error {
	req.prevLogIndex = repl.nextIndex - 1

	// fill req.prevLogTerm
	if req.prevLogIndex == 0 {
		req.prevLogTerm = 0
	} else {
		snapIndex, snapTerm := repl.getSnapLog()
		if req.prevLogIndex < snapTerm {
			return errNoEntryFound
		}
		if req.prevLogIndex == snapIndex {
			req.prevLogTerm = snapTerm
		} else if req.prevLogIndex >= repl.ldrStartIndex { // being smart!!!
			req.prevLogTerm = req.term
		} else {
			prevTerm, err := repl.storage.getEntryTerm(req.prevLogIndex)
			if err != nil {
				return err
			}
			req.prevLogTerm = prevTerm
		}
	}

	var n uint64
	if repl.sendEntries {
		assert(repl.matchIndex == req.prevLogIndex, "%s assert %d==%d", repl, repl.matchIndex, req.prevLogIndex)
		n = min(repl.ldrLastIndex-repl.matchIndex, maxAppendEntries)
	}
	if n > 0 {
		req.entries = make([]*entry, n)
		for i := range req.entries {
			req.entries[i] = &entry{}
			err := repl.storage.getEntry(repl.nextIndex+uint64(i), req.entries[i])
			if err != nil {
				return err
			}
		}
		debug(repl, "sending", req)
	} else {
		req.entries = nil
		if repl.sendEntries {
			debug(repl, "sending heartbeat")
		}
	}

	resp := &appendEntriesResp{}
	if err := repl.retryRPC(req, resp); err != nil {
		return err
	}

	repl.sendEntries = resp.success
	if resp.success {
		old := repl.matchIndex
		repl.matchIndex, _ = lastEntry(req)
		repl.nextIndex = repl.matchIndex + 1
		if repl.matchIndex != old {
			debug(repl, "matchIndex:", repl.matchIndex)
			repl.notifyLdr(matchIndex{&repl.status, repl.matchIndex})
		}
	} else {
		if resp.lastLogIndex < repl.matchIndex {
			// this happens if someone restarted follower storage with empty storage
			// todo: can we treat replicate entire snap+log to such follower ??
			return errors.New("faulty follower: denies matchIndex")
		}
		repl.nextIndex = min(repl.nextIndex-1, resp.lastLogIndex+1)
		debug(repl, "nextIndex:", repl.nextIndex)
	}
	return nil
}

func (repl *replication) sendInstallSnapReq(appReq *appendEntriesReq) error {
	req := &installLatestSnapReq{
		installSnapReq: installSnapReq{
			term:   appReq.term,
			leader: appReq.leader,
		},
		snapshots: repl.storage.snapshots,
	}

	resp := &installSnapResp{}
	if err := repl.retryRPC(req, resp); err != nil {
		return err
	}

	// we have to still send one appEntries, to update his commit index
	// so we should not update sendEntries=true, beacuse if we have
	// no entries beyond snapshot, we sleep for hbTimeout
	//repl.sendEntries = resp.success // NOTE: dont do this
	if resp.success {
		repl.matchIndex = req.lastIndex
		repl.nextIndex = repl.matchIndex + 1
		debug(repl, "matchIndex:", repl.matchIndex)
		repl.notifyLdr(matchIndex{&repl.status, repl.matchIndex})
		return nil
	} else {
		return errors.New("installSnap.success is false")
	}
}

func (repl *replication) retryRPC(req request, resp message) error {
	var failures uint64
	for {
		err := repl.doRPC(req, resp)
		if _, ok := err.(OpError); ok {
			return err
		} else if err != nil {
			if repl.noContact.IsZero() {
				repl.noContact = time.Now()
				debug(repl, "noContact", err)
				repl.notifyLdr(noContact{&repl.status, repl.noContact})
			}
			failures++
			select {
			case <-repl.stopCh:
				return errStop
			case <-time.After(backOff(failures)):
				continue
			}
		}
		break
	}
	if !repl.noContact.IsZero() {
		repl.noContact = time.Time{} // zeroing
		debug(repl, "yesContact")
		repl.notifyLdr(noContact{&repl.status, repl.noContact})
	}
	if resp.getTerm() > req.getTerm() {
		repl.notifyLdr(newTerm{resp.getTerm()})
		return errStop
	}
	return nil
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

func (repl *replication) notifyLdr(update interface{}) {
	select {
	case <-repl.stopCh:
	case repl.toLeaderCh <- update:
	}
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
		return opError(err, "Snapshots.Open")
	}
	req.lastIndex = meta.Index
	req.lastTerm = meta.Term
	req.lastConfig = meta.Config
	req.size = meta.Size
	req.snapshot = snapshot
	return req.installSnapReq.encode(w)
}

type leaderUpdate struct {
	lastIndex, commitIndex uint64
}

type matchIndex struct {
	status *replStatus
	val    uint64
}

type noContact struct {
	status *replStatus
	time   time.Time
}

type newTerm struct {
	val uint64
}

type replStatus struct {
	id ID

	// owned exclusively by leader goroutine
	// used to compute majorityMatchIndex
	matchIndex uint64

	// from what time the replication unable to reach this node
	// zero value means it is reachable
	noContact time.Time
}

// did we have success full contact after time t
func (rs *replStatus) contactedAfter(t time.Time) bool {
	return rs.noContact.IsZero() || rs.noContact.After(t)
}

// ------------------------------------------------

const (
	maxAppendEntries = 64 // todo: should be configurable
	maxFailureScale  = 12
	failureWait      = 10 * time.Millisecond
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
