package raft

import (
	"fmt"
	"io"
	"time"
)

type flr struct {
	rtime  randTime
	status flrStatus // owned by ldr goroutine

	connPool  *connPool
	storage   *storage
	hbTimeout time.Duration

	ldrStartIndex uint64
	ldrLastIndex  uint64
	matchIndex    uint64
	nextIndex     uint64

	node  Node
	round *Round // nil if no promotion required

	// from this time node is unreachable
	// zero value means node is reachable
	noContact time.Time

	fromLeaderCh chan leaderUpdate
	toLeaderCh   chan<- interface{}
	stopCh       chan struct{}

	trace *Trace // todo: trace should not be shared with ldrShip
	str   string // used for debug() calls
}

func (f *flr) replicate(req *appendEntriesReq) {
	debug(f, "f.start")
	if f.node.promote() {
		f.round = new(Round)
		f.round.begin(f.ldrLastIndex)
		debug(f, "started:", f.round)
	}

	var c *conn
	defer func() {
		if c != nil {
			f.connPool.returnConn(c)
		}
	}()

	timer, failures, err := newSafeTimer(), uint64(0), error(nil)
	for {
		if failures > 0 {
			if c != nil {
				_ = c.rwc.Close()
				c = nil
			}
			if f.noContact.IsZero() {
				f.noContact = time.Now()
				debug(f, "noContact", err)
				f.notifyLdr(noContact{&f.status, f.noContact, err})
			}
			select {
			case <-f.stopCh:
				return
			case <-time.After(backOff(failures)):
			}
		}

		if c == nil {
			if c, err = f.connPool.getConn(); err != nil {
				failures++
				continue
			}
			failures = 0
			if !f.noContact.IsZero() {
				f.noContact = time.Time{} // zeroing
				debug(f, "yesContact")
				f.notifyLdr(noContact{&f.status, f.noContact, nil})
			}
		}

		assert(f.matchIndex < f.nextIndex, "%v assert %d<%d", f, f.matchIndex, f.nextIndex)

		err = f.sendAppEntriesReq(c, req)

		if err == errStop {
			return
		} else if err != nil && err != errNoEntryFound {
			// todo: we need to distinguish between local/remote opErr
			//       only local opErr to be sent to trace
			if _, ok := err.(OpError); ok {
				if f.trace.Error != nil {
					f.trace.Error(err)
				}
			}
			failures++
			continue
		}

		if f.matchIndex == f.ldrLastIndex {
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

func (f *flr) sendAppEntriesReq(c *conn, req *appendEntriesReq) (err error) {
	req.prevLogIndex = f.nextIndex - 1

	// fill req.prevLogTerm
	f.storage.snapMu.RLock()
	snapIndex, snapTerm := f.storage.snapIndex, f.storage.snapTerm
	f.storage.snapMu.RUnlock()
	if req.prevLogIndex == snapIndex {
		req.prevLogTerm = snapTerm
	} else {
		req.prevLogTerm, err = f.storage.getEntryTerm(req.prevLogIndex)
		if err != nil { // meanwhile leader compacted log
			return f.sendInstallSnapReq(c, req)
		}
	}

	var n uint64
	if f.matchIndex+1 == f.nextIndex {
		assert(f.matchIndex == req.prevLogIndex, "%v assert %d==%d", f, f.matchIndex, req.prevLogIndex)
		n = min(f.ldrLastIndex-f.matchIndex, maxAppendEntries)
	}

	debug(f, ">>", req)
	if err := c.writeReq(req); err != nil {
		return err
	}
	e := &entry{index: req.prevLogIndex}
	for i := uint64(0); i < n; i++ {
		if err := f.storage.getEntry(f.nextIndex+i, e); err != nil {
			break
		}
		if err := writeUint8(c.bufw, 1); err != nil {
			return err
		}
		if err = e.encode(c.bufw); err != nil {
			return err
		}
	}
	if err := writeUint8(c.bufw, appendEOF); err != nil {
		return err
	}
	if err := c.bufw.Flush(); err != nil {
		return err
	}

	resp := &appendEntriesResp{}
	if err := c.readResp(resp); err != nil {
		return err
	}
	debug(f, "<<", resp)
	switch resp.result {
	case staleTerm:
		f.notifyLdr(newTerm{resp.getTerm()})
		return errStop
	case success:
		old := f.matchIndex
		f.matchIndex = e.index
		f.nextIndex = f.matchIndex + 1
		if f.matchIndex != old {
			debug(f, "matchIndex:", f.matchIndex)
			f.notifyLdr(matchIndex{&f.status, f.matchIndex})
		}
		return nil
	case prevEntryNotFound, prevTermMismatch:
		if resp.lastLogIndex < f.matchIndex {
			// this happens if someone restarted follower storage with empty storage
			// todo: can we treat replicate entire snap+log to such follower ??
			return ErrFaultyFollower
		}
		f.nextIndex = min(f.nextIndex-1, resp.lastLogIndex+1)
		debug(f, "nextIndex:", f.nextIndex)
		return nil
	default:
		return ErrRemote
	}
}

func (f *flr) sendInstallSnapReq(c *conn, appReq *appendEntriesReq) error {
	meta, snapshot, err := f.storage.snapshots.Open()
	if err != nil {
		return opError(err, "Snapshots.Open")
	}
	defer snapshot.Close()

	req := &installSnapReq{
		req:        appReq.req,
		lastIndex:  meta.Index,
		lastTerm:   meta.Term,
		lastConfig: meta.Config,
		size:       meta.Size,
	}
	debug(f, ">>", req)
	if err = c.writeReq(req); err != nil {
		return err
	}
	if _, err = io.CopyN(c.rwc, snapshot, req.size); err != nil {
		return err
	}
	if err = c.bufw.Flush(); err != nil {
		return err
	}

	resp := &installSnapResp{}
	if err = c.readResp(resp); err != nil {
		return err
	}
	switch resp.result {
	case staleTerm:
		f.notifyLdr(newTerm{resp.getTerm()})
		return errStop
	case success:
		f.matchIndex = req.lastIndex
		f.nextIndex = f.matchIndex + 1
		debug(f, "matchIndex:", f.matchIndex, "nextIndex:", f.nextIndex)
		f.notifyLdr(matchIndex{&f.status, f.matchIndex})
		return nil
	default:
		return ErrRemote
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
