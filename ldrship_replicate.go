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
	timer     *safeTimer

	ldrStartIndex uint64
	ldrLastIndex  uint64
	matchIndex    uint64
	nextIndex     uint64

	voter bool

	// from this time node is unreachable
	// zero value means node is reachable
	noContact time.Time

	fromLeaderCh chan leaderUpdate
	toLeaderCh   chan<- interface{}
	stopCh       chan struct{}

	str string // used for debug() calls
}

func (f *flr) runLoop(req *appendEntriesReq) {
	debug(f, "f.start")
	var c *conn
	defer func() {
		if c != nil && c.rwc != nil {
			f.connPool.returnConn(c)
		}
		if v := recover(); v != nil {
			debug(f, "got panic:", v)
			f.notifyLdr(toErr(v))
		}
	}()

	failures, err := uint64(0), error(nil)
	for {
		if failures > 0 {
			if failures == 1 {
				f.notifyNoContact(err)
			}
			f.timer.reset(backOff(failures))
			select {
			case <-f.stopCh:
				return
			case <-f.timer.C:
				f.timer.active = false
			}
		}

		if c == nil {
			if c, err = f.connPool.getConn(); err != nil {
				failures++
				continue
			}
			if failures > 0 {
				failures = 0
				f.notifyNoContact(nil)

				// we have not checked leader update since no contact
				// let us check it
				_, _ = f.checkLeaderUpdate(f.stopCh, req, false)
			}
		}

		assert(f.matchIndex < f.nextIndex, "%v assert %d<%d", f, f.matchIndex, f.nextIndex)
		err = f.replicate(c, req)
		if err == errStop {
			return
		} else if _, ok := err.(OpError); ok {
			panic(err)
		} else if remoteErr, ok := err.(remoteError); ok {
			err = remoteErr.error
		}
		failures++
		if c.rwc != nil {
			_ = c.rwc.Close()
		}
		c = nil
	}
}

// always returns non-nil error
func (f *flr) replicate(c *conn, req *appendEntriesReq) (err error) {
	resp := &appendEntriesResp{}
	for {
		// find matchIndex ---------------------------------------------------
		for {
			err = f.writeAppendEntriesReq(c, req, false)
			if err == errNoEntryFound {
				if err = f.sendInstallSnapReq(c, req); err == nil {
					continue
				}
			}
			if err != nil {
				return err
			}

			if err = c.readResp(resp); err != nil {
				return err
			}
			if err = f.onAppendEntriesResp(resp, f.nextIndex-1); err != nil {
				return err
			}
			if _, err = f.checkLeaderUpdate(f.stopCh, req, false); err != nil {
				return err
			}
			if f.matchIndex+1 == f.nextIndex {
				break
			}
		}

		debug(f, "pipelining.............................")
		// pipelining ---------------------------------------------------------
		type result struct {
			lastIndex uint64
			err       error
		}
		var (
			resultCh = make(chan result, 128)
			stopCh   = make(chan struct{})
		)
		go func() {
			defer func() {
				close(resultCh)
				if v := recover(); v != nil {
					select {
					case <-stopCh:
						return
					case resultCh <- result{0, toErr(v)}:
					}
				}
			}()
			for {
				err := f.writeAppendEntriesReq(c, req, true)
				select {
				case <-stopCh:
					return
				case resultCh <- result{f.nextIndex - 1, err}:
				}
				if err != nil {
					return
				}
				for {
					ldrUpdate, errStop := f.checkLeaderUpdate(stopCh, req, true)
					if errStop != nil {
						return
					}
					if ldrUpdate || len(resultCh) == 0 || f.nextIndex <= f.ldrLastIndex {
						break
					}
					// we have not received responses for all requests yet
					// so no need to flood him with heartbeat
					// take a nap and check again
				}
			}
		}()
		for {
			var result result
			select {
			case <-f.stopCh:
				err = errStop
			case result = <-resultCh:
				if result.err != nil {
					debug(f, "pipeline ended with", result.err)
					if result.err == errNoEntryFound {
						break
					}
					return result.err
				}
				err = c.readResp(resp)
			}
			if err != nil {
				debug(f, "ending pipeline, got", err)
				close(stopCh)
				// wait for pipeline routine finish
				for range resultCh {
				}
				return err
			}
			if resp.result == success {
				_ = f.onAppendEntriesResp(resp, result.lastIndex)
				continue
			}

			debug(f, "ending pipeline, got resp.result", resp.result)
			close(stopCh)
			drainResps := func() error {
				for range resultCh {
					if err = c.readResp(resp); err != nil {
						return err
					}
				}
				return nil
			}

			if resp.result == staleTerm {
				drained := make(chan error, 1)
				go func() {
					drained <- drainResps()
				}()
				closed := false
				select {
				case err = <-drained:
				case <-time.After(f.hbTimeout / 2):
					closed, _, err = true, c.rwc.Close(), io.ErrClosedPipe
					<-drained
				}
				if err != nil {
					if !closed {
						_ = c.rwc.Close()
					}
					c.rwc = nil // to signal runLoop that we closed the conn
				}
				return f.onAppendEntriesResp(resp, result.lastIndex) // notifies ldr and return errStop
			}

			if err = drainResps(); err != nil {
				return err
			}
			break
		}
	}
}

const maxAppendEntries = 64

func (f *flr) writeAppendEntriesReq(c *conn, req *appendEntriesReq, sendEntries bool) (err error) {
	f.storage.prevLogMu.RLock()
	defer f.storage.prevLogMu.RUnlock()

	snapIndex, snapTerm := f.storage.snaps.latest()

	// fill req.prevLogXXX
	req.prevLogIndex = f.nextIndex - 1
	if req.prevLogIndex == 0 {
		req.prevLogTerm = 0
	} else if req.prevLogIndex == snapIndex {
		req.prevLogTerm = snapTerm
	} else {
		req.prevLogTerm, err = f.storage.getEntryTerm(req.prevLogIndex)
		if err != nil { // meanwhile leader compacted log
			return
		}
	}

	req.numEntries = 0
	if sendEntries {
		req.numEntries = min(f.ldrLastIndex-req.prevLogIndex, maxAppendEntries)
	}

	if sendEntries && req.numEntries == 0 {
		debug(f, ">> heartbeat")
	} else {
		debug(f, ">>", req)
	}
	if err = c.writeReq(req); err != nil {
		return
	}
	if req.numEntries > 0 {
		if err = f.storage.WriteEntriesTo(c.bufw, f.nextIndex, req.numEntries); err != nil {
			return
		}
		if err = c.bufw.Flush(); err != nil {
			return
		}
		f.nextIndex += req.numEntries
		debug(f, "nextIndex:", f.nextIndex)
	}
	return nil
}

func (f *flr) onAppendEntriesResp(resp *appendEntriesResp, reqLastIndex uint64) error {
	debug(f, "<<", resp)
	switch resp.result {
	case staleTerm:
		f.notifyLdr(newTerm{resp.getTerm()})
		return errStop
	case success:
		if reqLastIndex > f.matchIndex {
			f.matchIndex = reqLastIndex
			debug(f, "matchIndex:", f.matchIndex)
			f.notifyLdr(matchIndex{&f.status, f.matchIndex})
		}
		return nil
	case prevEntryNotFound, prevTermMismatch:
		if resp.lastLogIndex < f.matchIndex {
			// this happens if someone restarted follower storage with empty storage
			return ErrFaultyFollower
		}
		f.nextIndex = min(f.nextIndex-1, resp.lastLogIndex+1)
		debug(f, "nextIndex:", f.nextIndex)
		return nil
	case unexpectedErr:
		return remoteError{resp.err}
	default:
		panic(fmt.Errorf("[BUG] appendEntries.result==%v", resp.result))
	}
}

func (f *flr) sendInstallSnapReq(c *conn, appReq *appendEntriesReq) error {
	meta, snapshot, err := f.storage.snaps.open()
	if err != nil {
		return opError(err, "snapshots.open")
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
	if _, err = io.CopyN(c.bufw, snapshot, req.size); err != nil {
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
		// case: snapshot was taken before we got leaderUpdate about lastLogIndex
		if req.lastIndex > f.ldrLastIndex {
			f.ldrLastIndex = req.lastIndex
		}
		f.matchIndex = req.lastIndex
		f.nextIndex = f.matchIndex + 1
		debug(f, "matchIndex:", f.matchIndex, "nextIndex:", f.nextIndex)
		f.notifyLdr(matchIndex{&f.status, f.matchIndex})
		return nil
	case unexpectedErr:
		return remoteError{resp.err}
	default:
		panic(fmt.Errorf("[BUG] installSnapResp.result==%v", resp.result))
	}
}

func (f *flr) checkLeaderUpdate(stopCh <-chan struct{}, req *appendEntriesReq, sendEntries bool) (ldrUpdate bool, err error) {
	if sendEntries && f.nextIndex > f.ldrLastIndex {
		// for nonvoter, dont send heartbeats
		var timerCh <-chan time.Time
		if f.voter {
			f.timer.reset(f.hbTimeout / 2)
			timerCh = f.timer.C
		}

		// nothing to send. start heartbeat timer
		select {
		case <-stopCh:
			return false, errStop
		case update := <-f.fromLeaderCh:
			f.onLeaderUpdate(update, req)
			return true, nil
		case <-timerCh:
			f.timer.active = false
			return false, nil
		}
	} else {
		// check signal if any, without blocking
		select {
		case <-stopCh:
			return false, errStop
		case update := <-f.fromLeaderCh:
			f.onLeaderUpdate(update, req)
			return true, nil
		default:
		}
	}
	return false, nil
}

func (f *flr) onLeaderUpdate(u leaderUpdate, req *appendEntriesReq) {
	debug(f, u)
	f.ldrLastIndex, req.ldrCommitIndex = u.lastIndex, u.commitIndex
	if u.config != nil {
		f.voter = u.config.Nodes[f.status.id].Voter
	}
}

func (f *flr) notifyLdr(u interface{}) {
	select {
	case <-f.stopCh:
	case f.toLeaderCh <- u:
	}
}

func (f *flr) notifyNoContact(err error) {
	if err != nil {
		f.noContact = time.Now()
		debug(f, "noContact", err)
		f.notifyLdr(noContact{&f.status, f.noContact, err})
	} else {
		f.noContact = time.Time{} // zeroing
		debug(f, "yesContact")
		f.notifyLdr(noContact{&f.status, f.noContact, nil})
	}
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

type flrStatus struct {
	id uint64

	// owned exclusively by leader goroutine
	// used to compute majorityMatchIndex
	matchIndex uint64

	// from what time the replication unable to reach this node
	// zero value means it is reachable
	noContact time.Time

	err error

	round *Round // nil if no promotion required
}
