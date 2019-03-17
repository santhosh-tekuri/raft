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
	conn      *conn

	ldrStartIndex uint64
	ldrLastIndex  uint64
	matchIndex    uint64
	nextIndex     uint64
	sendEntries   bool

	node  Node
	round *Round // nil if no promotion reqd

	// from this time node is unreachable
	// zero value means node is reachable
	noContact time.Time

	// leader notifies flr with update
	fromLeaderCh chan leaderUpdate

	// flr notifies leader about its progress
	toLeaderCh chan<- interface{}
	stopCh     chan struct{}

	trace *Trace // todo: trace should not be shared with ldrShip
	str   string // used for debug() calls
}

func (f *flr) replicate(req *appendEntriesReq) {
	defer func() {
		if f.conn != nil {
			f.connPool.returnConn(f.conn)
		}
	}()

	debug(f, "f.start")
	if f.node.promote() {
		f.round = new(Round)
		f.round.begin(f.ldrLastIndex)
		debug(f, "started:", f.round)
	}

	timer := newSafeTimer()
	for {
		assert(f.matchIndex < f.nextIndex, "%v assert %d<%d", f, f.matchIndex, f.nextIndex)

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
			fatal("raft.replicate: %v", err) // todo
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
				timer.active = false
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

func (f *flr) onLeaderUpdate(u leaderUpdate, req *appendEntriesReq) {
	debug(f, u)
	f.ldrLastIndex, req.ldrCommitIndex = u.lastIndex, u.commitIndex
	if u.config != nil {
		if n, ok := u.config.Nodes[f.status.id]; ok {
			f.node = n
			if !f.node.promote() {
				f.round = nil
			} else if f.round == nil {
				// start first round
				f.round = new(Round)
				f.round.begin(f.ldrLastIndex)
				debug(f, "started:", f.round)
			}
		}
	}

	// if round was completed
	if f.round != nil && f.round.finished() {
		if f.ldrLastIndex > f.round.LastIndex {
			f.round.begin(f.ldrLastIndex)
			debug(f, "started:", f.round)
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
		assert(f.matchIndex == req.prevLogIndex, "%v assert %d==%d", f, f.matchIndex, req.prevLogIndex)
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
	} else {
		req.entries = nil
	}

	if req.entries != nil || !f.sendEntries {
		debug(f, ">>", req)
	} else {
		debug(f, ">> heartbeat")
	}
	resp := &appendEntriesResp{}
	if err := f.retryRPC(req, resp); err != nil {
		return err
	}
	if req.entries != nil || !f.sendEntries {
		debug(f, "<<", resp)
	}

	f.sendEntries = resp.result == success
	if resp.result == success {
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
		installSnapReq: installSnapReq{req: appReq.req},
		snapshots:      f.storage.snapshots,
	}

	debug(f, "sending installReq")
	resp := &installSnapResp{}
	if err := f.retryRPC(req, resp); err != nil {
		return err
	}

	// we have to still send one appEntries, to update his commit index
	// so we should not update sendEntries=true, beacuse if we have
	// no entries beyond snapshot, we sleep for hbTimeout
	//f.sendEntries = resp.success // NOTE: dont do this
	if resp.result == success {
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
				f.notifyLdr(noContact{&f.status, f.noContact, err})
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
		if !f.noContact.IsZero() {
			f.noContact = time.Time{} // zeroing
			debug(f, "yesContact")
			f.notifyLdr(noContact{&f.status, f.noContact, nil})
		}
	}
	if f.trace.sending != nil {
		f.trace.sending(req.from(), f.connPool.id, Leader, req)
	}
	err := f.conn.doRPC(req, resp)
	if err != nil {
		_ = f.conn.rwc.Close()
		f.conn = nil
	}
	if f.trace.received != nil && err == nil {
		f.trace.received(req.from(), f.connPool.id, Leader, req.getTerm(), resp)
	}
	return err
}

func (f *flr) notifyLdr(u interface{}) {
	select {
	case <-f.stopCh:
	case f.toLeaderCh <- u:
	}

	// check if we just completed round
	if _, ok := u.(matchIndex); ok && f.round != nil {
		if f.matchIndex >= f.round.LastIndex {
			f.notifyRoundCompleted()
		}
	}
}

func (f *flr) notifyRoundCompleted() {
	if !f.round.finished() {
		f.round.finish()
	}
	debug(f, "notify completed:", f.round)
	update := roundCompleted{&f.status, *f.round}
	select {
	case <-f.stopCh:
	case f.toLeaderCh <- update:
	}
	// if any entries still left, start next round
	if f.ldrLastIndex > f.round.LastIndex {
		f.round.begin(f.ldrLastIndex)
		debug(f, "started:", f.round)
	}
}

func (f *flr) getSnapLog() (snapIndex, snapTerm uint64) {
	// snapshoting might be in progress
	f.storage.snapMu.RLock()
	defer f.storage.snapMu.RUnlock()
	return f.storage.snapIndex, f.storage.snapTerm
}

func lastEntry(req *appendEntriesReq) (index, term uint64) {
	if n := len(req.entries); n == 0 {
		return req.prevLogIndex, req.prevLogTerm
	} else {
		last := req.entries[n-1]
		return last.index, last.term
	}
}

// ------------------------------------------------

type Round struct {
	Ordinal   uint64
	Start     time.Time
	End       time.Time
	LastIndex uint64
}

func (r *Round) begin(lastIndex uint64) {
	r.Ordinal, r.Start, r.LastIndex = r.Ordinal+1, time.Now(), lastIndex
}
func (r *Round) finish()                { r.End = time.Now() }
func (r *Round) finished() bool         { return !r.End.IsZero() }
func (r Round) Duration() time.Duration { return r.End.Sub(r.Start) }

func (r Round) String() string {
	if r.finished() {
		return fmt.Sprintf("round{#%d %s lastIndex: %d}", r.Ordinal, r.Duration(), r.LastIndex)
	}
	return fmt.Sprintf("round{#%d lastIndex: %d}", r.Ordinal, r.LastIndex)
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
	defer snapshot.Close()
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
	err    error
}

type newTerm struct {
	val uint64
}

type roundCompleted struct {
	status *flrStatus
	round  Round
}

type flrStatus struct {
	id uint64

	// owned exclusively by leader goroutine
	// used to compute majorityMatchIndex
	matchIndex uint64

	// from what time the replication unable to reach this node
	// zero value means it is reachable
	noContact time.Time

	rounds uint64 // #rounds completed
}
