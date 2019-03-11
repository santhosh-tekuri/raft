package raft

import (
	"errors"
	"fmt"
	"io"
	"time"
)

type flr struct {
	rtime randTime

	// this is owned by ldr goroutine
	status flrStatus

	connPool  *connPool
	storage   *storage
	hbTimeout time.Duration
	conn      *netConn

	ldrStartIndex uint64
	ldrLastIndex  uint64
	matchIndex    uint64
	nextIndex     uint64
	sendEntries   bool

	node  Node
	round *round // nil if no promotion reqd

	// from this time node is unreachable
	// zero value means node is reachable
	noContact time.Time

	// leader notifies flr with update
	fromLeaderCh chan leaderUpdate

	// flr notifies leader about its progress
	toLeaderCh chan<- interface{}
	stopCh     chan struct{}

	trace *Trace
	str   string // used for debug() calls
}

func (f *flr) replicate(req *appendEntriesReq) {
	defer func() {
		if f.conn != nil {
			f.connPool.returnConn(f.conn)
		}
	}()

	debug(f, "f.start")
	f.ldrLastIndex = req.prevLogIndex
	f.matchIndex, f.nextIndex = uint64(0), f.ldrLastIndex+1
	if f.node.promote() {
		f.round = new(round)
		f.round.begin(f.ldrLastIndex)
		debug(f, "started round:", f.round)
	}

	timer := newSafeTimer()
	for {
		debug(f, "matchIndex", f.matchIndex, "nextIndex", f.nextIndex)
		assert(f.matchIndex < f.nextIndex, "%s assert %d<%d", f, f.matchIndex, f.nextIndex)

		err := f.sendAppEntriesReq(req)
		if err == errNoEntryFound {
			err = f.sendInstallSnapReq(req)
		}

		if err == errStop {
			return
		} else if err != nil {
			if f.trace.Error != nil {
				f.trace.Error(err)
			}
			assert(false, "unexpected error: %v", err) // todo
			continue
		}

		if f.sendEntries && f.matchIndex == f.ldrLastIndex {
			timer.reset(f.hbTimeout / 10)
			// nothing to send. start heartbeat timer
			select {
			case <-f.stopCh:
				return
			case update := <-f.fromLeaderCh:
				f.onLeaderUpdate(update, req)
			case <-timer.C:
				timer.receive = false
			}
			timer.stop()
		} else {
			// check signal if any, without blocking
			select {
			case <-f.stopCh:
				return
			case update := <-f.fromLeaderCh:
				f.onLeaderUpdate(update, req)
			default:
			}
		}
	}
}

func (f *flr) onLeaderUpdate(update leaderUpdate, req *appendEntriesReq) {
	debug(f, "{last:", update.lastIndex, "commit:", update.commitIndex, "config:", update.config, "} <-fromLeaderCh")
	f.ldrLastIndex, req.ldrCommitIndex = update.lastIndex, update.commitIndex
	if update.config != nil {
		if n, ok := update.config.Nodes[f.status.id]; ok {
			f.node = n
			if !f.node.promote() {
				f.round = nil
			} else if f.round == nil {
				// start first round
				f.round = new(round)
				f.round.begin(f.ldrLastIndex)
				debug(f, "started round:", f.round)
			}
		}
	}

	// if round was completed
	if f.round != nil && f.round.finished() {
		if f.ldrLastIndex > f.round.lastIndex {
			f.round.begin(f.ldrLastIndex)
			debug(f, "started round:", f.round)
		} else {
			debug(f, "reminding leader about promotion")
			f.notifyRoundCompleted() // no new entries, reminding leader about promotion
		}
	}
}

func (f *flr) sendAppEntriesReq(req *appendEntriesReq) error {
	req.prevLogIndex = f.nextIndex - 1

	// fill req.prevLogTerm
	if req.prevLogIndex == 0 {
		req.prevLogTerm = 0
	} else {
		snapIndex, snapTerm := f.getSnapLog()
		if req.prevLogIndex < snapTerm {
			return errNoEntryFound
		}
		if req.prevLogIndex == snapIndex {
			req.prevLogTerm = snapTerm
		} else if req.prevLogIndex >= f.ldrStartIndex { // being smart!!!
			req.prevLogTerm = req.term
		} else {
			prevTerm, err := f.storage.getEntryTerm(req.prevLogIndex)
			if err != nil {
				return err
			}
			req.prevLogTerm = prevTerm
		}
	}

	var n uint64
	if f.sendEntries {
		assert(f.matchIndex == req.prevLogIndex, "%s assert %d==%d", f, f.matchIndex, req.prevLogIndex)
		n = min(f.ldrLastIndex-f.matchIndex, maxAppendEntries)
	}
	if n > 0 {
		req.entries = make([]*entry, n)
		for i := range req.entries {
			req.entries[i] = &entry{}
			err := f.storage.getEntry(f.nextIndex+uint64(i), req.entries[i])
			if err != nil {
				return err
			}
		}
		debug(f, "sending", req)
	} else {
		req.entries = nil
		if f.sendEntries {
			debug(f, "sending heartbeat")
		}
	}

	resp := &appendEntriesResp{}
	if err := f.retryRPC(req, resp); err != nil {
		return err
	}

	f.sendEntries = resp.success
	if resp.success {
		old := f.matchIndex
		f.matchIndex, _ = lastEntry(req)
		f.nextIndex = f.matchIndex + 1
		if f.matchIndex != old {
			debug(f, "matchIndex:", f.matchIndex)
			f.notifyLdr(matchIndex{&f.status, f.matchIndex})
		}
	} else {
		if resp.lastLogIndex < f.matchIndex {
			// this happens if someone restarted follower storage with empty storage
			// todo: can we treat replicate entire snap+log to such follower ??
			return errors.New("faulty follower: denies matchIndex")
		}
		f.nextIndex = min(f.nextIndex-1, resp.lastLogIndex+1)
		debug(f, "nextIndex:", f.nextIndex)
	}
	return nil
}

func (f *flr) sendInstallSnapReq(appReq *appendEntriesReq) error {
	req := &installLatestSnapReq{
		installSnapReq: installSnapReq{
			term:   appReq.term,
			leader: appReq.leader,
		},
		snapshots: f.storage.snapshots,
	}

	resp := &installSnapResp{}
	if err := f.retryRPC(req, resp); err != nil {
		return err
	}

	// we have to still send one appEntries, to update his commit index
	// so we should not update sendEntries=true, beacuse if we have
	// no entries beyond snapshot, we sleep for hbTimeout
	//f.sendEntries = resp.success // NOTE: dont do this
	if resp.success {
		f.matchIndex = req.lastIndex
		f.nextIndex = f.matchIndex + 1
		debug(f, "matchIndex:", f.matchIndex)
		f.notifyLdr(matchIndex{&f.status, f.matchIndex})
		return nil
	} else {
		return errors.New("installSnap.success is false")
	}
}

func (f *flr) retryRPC(req request, resp message) error {
	var failures uint64
	for {
		err := f.doRPC(req, resp)
		if _, ok := err.(OpError); ok {
			return err
		} else if err != nil {
			if f.noContact.IsZero() {
				f.noContact = time.Now()
				debug(f, "noContact", err)
				f.notifyLdr(noContact{&f.status, f.noContact})
			}
			failures++
			select {
			case <-f.stopCh:
				return errStop
			case <-time.After(backOff(failures)):
				continue
			}
		}
		break
	}
	if !f.noContact.IsZero() {
		f.noContact = time.Time{} // zeroing
		debug(f, "yesContact")
		f.notifyLdr(noContact{&f.status, f.noContact})
	}
	if resp.getTerm() > req.getTerm() {
		f.notifyLdr(newTerm{resp.getTerm()})
		return errStop
	}
	return nil
}

func (f *flr) doRPC(req request, resp message) error {
	if f.conn == nil {
		conn, err := f.connPool.getConn()
		if err != nil {
			return err
		}
		f.conn = conn
	}
	if f.trace.sending != nil {
		f.trace.sending(req.from(), f.connPool.id, req)
	}
	err := f.conn.doRPC(req, resp)
	if err != nil {
		_ = f.conn.close()
		f.conn = nil
	}
	if f.trace.sending != nil && err == nil {
		f.trace.received(req.from(), f.connPool.id, resp)
	}
	return err
}

func (f *flr) notifyLdr(update interface{}) {
	select {
	case <-f.stopCh:
	case f.toLeaderCh <- update:
	}

	// check if we just completed round
	if _, ok := update.(matchIndex); ok && f.round != nil {
		if f.matchIndex >= f.round.lastIndex {
			f.notifyRoundCompleted()
		}
	}
}

func (f *flr) notifyRoundCompleted() {
	if !f.round.finished() {
		f.round.finish()
	}
	debug(f, "notify roundCompleted:", f.round)
	update := roundCompleted{&f.status, *f.round}
	select {
	case <-f.stopCh:
	case f.toLeaderCh <- update:
	}
	// if any entries still left, start next round
	if f.ldrLastIndex > f.round.lastIndex {
		f.round.begin(f.ldrLastIndex)
		debug(f, "started round:", f.round)
	}
}

func (f *flr) getSnapLog() (snapIndex, snapTerm uint64) {
	// snapshoting might be in progress
	f.storage.snapMu.RLock()
	defer f.storage.snapMu.RUnlock()
	return f.storage.snapIndex, f.storage.snapTerm
}

func (f *flr) String() string {
	return f.str
}

// ------------------------------------------------

type round struct {
	id        uint64
	start     time.Time
	end       time.Time
	lastIndex uint64
}

func (r *round) begin(lastIndex uint64) {
	r.id, r.start, r.lastIndex = r.id+1, time.Now(), lastIndex
}
func (r *round) duration() time.Duration { return r.end.Sub(r.start) }
func (r *round) finish()                 { r.end = time.Now() }
func (r *round) finished() bool          { return !r.end.IsZero() }

func (r round) String() string {
	if r.finished() {
		return fmt.Sprintf("%d %s lastIndex: %d", r.id, r.duration(), r.lastIndex)
	}
	return fmt.Sprintf("%d lastIndex: %d", r.id, r.lastIndex)
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
	lastIndex   uint64
	commitIndex uint64
	config      *Config // nil if config not changed
}

type matchIndex struct {
	status *flrStatus
	val    uint64
}

type noContact struct {
	status *flrStatus
	time   time.Time
}

type newTerm struct {
	val uint64
}

type roundCompleted struct {
	status *flrStatus
	round  round
}

type flrStatus struct {
	id ID

	// owned exclusively by leader goroutine
	// used to compute majorityMatchIndex
	matchIndex uint64

	// from what time the replication unable to reach this node
	// zero value means it is reachable
	noContact time.Time

	rounds uint64 // #rounds completed
}

// did we have success full contact after time t
func (rs *flrStatus) contactedAfter(t time.Time) bool {
	return rs.noContact.IsZero() || rs.noContact.After(t)
}
