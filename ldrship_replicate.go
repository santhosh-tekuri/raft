package raft

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"runtime"
	"time"

	"github.com/santhosh-tekuri/raft/log"
)

type flr struct {
	rtime  randTime
	status flrStatus // owned by ldr goroutine

	connPool  *connPool
	log       *log.Log
	snaps     *snapshots
	hbTimeout time.Duration
	timer     *safeTimer

	ldrStartIndex uint64
	ldrLastIndex  uint64 // todo: directly use log.lastIndex
	matchIndex    uint64
	nextIndex     uint64

	voter bool

	// from this time node is unreachable
	// zero value means node is reachable
	noContact time.Time

	leaderUpdateCh chan leaderUpdate
	replUpdateCh   chan<- replUpdate
	stopCh         chan struct{}

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
			if _, ok := v.(runtime.Error); ok {
				panic(v)
			}
			f.notifyLdr(toErr(v))
		}
	}()

	failures, err := uint64(0), error(nil)
	for {
		if failures > 0 {
			if failures == 1 {
				f.notifyNoContact(err)
			}
			f.timer.reset(backOff(failures, f.hbTimeout/2))
			select {
			case <-f.stopCh:
				return
			case <-f.timer.C:
				f.timer.active = false
			}
			if _, err = f.checkLeaderUpdate(f.stopCh, req, false); err == errStop {
				return
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
				if _, err = f.checkLeaderUpdate(f.stopCh, req, false); err == errStop {
					return
				}
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
func (f *flr) replicate(c *conn, req *appendEntriesReq) error {
	resp := &appendEntriesResp{}
	for {
		// find matchIndex ---------------------------------------------------
		for {
			err := f.writeAppendEntriesReq(c, req, false)
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

		if f.nextIndex < f.ldrLastIndex && !f.log.Contains(f.nextIndex) {
			if err := f.sendInstallSnapReq(c, req); err == nil {
				continue
			}
		}

		// todo: before starting pipeline, check if sending snap
		//       is better than sending lots of entries

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
					if _, ok := v.(runtime.Error); ok {
						panic(v)
					}
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
					debug(f, "no heartbeat, pending resps:", len(resultCh))
				}
			}
		}()

		drainResps := func() error {
			for range resultCh {
				if err := c.readResp(resp); err != nil {
					return err
				}
			}
			return nil
		}
		drainRespsTimeout := func(timeout time.Duration) {
			drained := make(chan error, 1)
			go func() {
				drained <- drainResps()
			}()
			select {
			case err := <-drained:
				debug(f, "drain completed with error:", err)
				if err != nil {
					_ = c.rwc.Close()
					c.rwc = nil // to signal runLoop that we closed the conn
				}
			case <-time.After(timeout):
				debug(f, "drain timeout, closing conn")
				_ = c.rwc.Close()
				<-drained
				debug(f, "drain completed")
				c.rwc = nil // to signal runLoop that we closed the conn
			}
		}

		var err error
		for {
			var result result
			select {
			case <-f.stopCh:
				debug(f, "ending pipeline, got stop signal from ldr")
				close(stopCh)
				drainRespsTimeout(f.hbTimeout / 2)
				return errStop
			case result = <-resultCh:
			}
			if result.err != nil {
				debug(f, "pipeline ended with", result.err)
				if result.err == errNoEntryFound {
					break
				}
				return result.err
			}
			err = c.readResp(resp)
			if err != nil {
				debug(f, "ending pipeline, error in reading resp:", err)
				close(stopCh)
				_ = c.rwc.Close()
				for range resultCh {
				}
				c.rwc = nil
				return err
			}
			if resp.result == success {
				_ = f.onAppendEntriesResp(resp, result.lastIndex)
			} else {
				debug(f, "ending pipeline, got resp.result", resp.result)
				close(stopCh)
				if resp.result == staleTerm {
					drainRespsTimeout(f.hbTimeout / 2)
					return f.onAppendEntriesResp(resp, result.lastIndex) // notifies ldr and return errStop
				} else {
					if err = drainResps(); err != nil {
						return err
					}
					break
				}
			}
		}
	}
}

const maxAppendEntries = 64

// note: never access f.matchIndex in this method, because this is used by pipeline writer also
func (f *flr) writeAppendEntriesReq(c *conn, req *appendEntriesReq, sendEntries bool) error {
	snapIndex, snapTerm := f.snaps.latest()

	// fill req.prevLogXXX
	req.prevLogIndex = f.nextIndex - 1
	if req.prevLogIndex == 0 {
		req.prevLogTerm = 0
	} else if req.prevLogIndex == snapIndex {
		req.prevLogTerm = snapTerm
	} else {
		term, err := f.getEntryTerm(req.prevLogIndex)
		if err != nil { // should be in snapshot
			return err
		}
		req.prevLogTerm = term
	}

	req.numEntries = 0
	if sendEntries {
		req.numEntries = min(f.ldrLastIndex-req.prevLogIndex, maxAppendEntries)
		if req.numEntries > 0 && !f.log.Contains(f.nextIndex) {
			return errNoEntryFound
		}
	}

	if sendEntries && req.numEntries == 0 {
		debug(f, ">> heartbeat")
	} else {
		debug(f, ">>", req)
	}
	if err := c.writeReq(req); err != nil {
		return err
	}
	if req.numEntries > 0 {
		if err := f.writeEntriesTo(c, f.nextIndex, req.numEntries); err != nil {
			return err
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
			f.notifyLdr(matchIndex{f.matchIndex})
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
	snap, err := f.snaps.open()
	if err != nil {
		return opError(err, "snapshots.open")
	}
	defer snap.release()

	req := &installSnapReq{
		req:        appReq.req,
		lastIndex:  snap.meta.index,
		lastTerm:   snap.meta.term,
		lastConfig: snap.meta.config,
		size:       snap.meta.size,
	}
	debug(f, ">>", req)
	if err = c.writeReq(req); err != nil {
		return err
	}
	if _, err = io.Copy(c.rwc, snap.file); err != nil { // will use sendFile
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
		// we should wait until we get our logview gets updated
		for req.lastIndex > f.ldrLastIndex {
			if _, err := f.checkLeaderUpdate(f.stopCh, appReq, false); err != nil {
				return err
			}
		}
		f.matchIndex = req.lastIndex
		f.nextIndex = f.matchIndex + 1
		debug(f, "matchIndex:", f.matchIndex, "nextIndex:", f.nextIndex)
		f.notifyLdr(matchIndex{f.matchIndex})
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
		case update := <-f.leaderUpdateCh:
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
		case update := <-f.leaderUpdateCh:
			f.onLeaderUpdate(update, req)
			return true, nil
		default:
		}
	}
	return false, nil
}

func (f *flr) onLeaderUpdate(u leaderUpdate, req *appendEntriesReq) {
	debug(f, u)
	if u.log.PrevIndex() > f.log.PrevIndex() {
		f.notifyLdr(removeLTE{u.log.PrevIndex()})
	}
	f.log = u.log
	f.ldrLastIndex, req.ldrCommitIndex = u.log.LastIndex(), u.commitIndex
	if u.config != nil {
		f.voter = u.config.Nodes[f.status.id].Voter
	}
}

func (f *flr) notifyLdr(u interface{}) {
	select {
	case <-f.stopCh:
	case f.replUpdateCh <- replUpdate{&f.status, u}:
	}
}

func (f *flr) notifyNoContact(err error) {
	if err != nil {
		f.noContact = time.Now()
		debug(f, "noContact", err)
		f.notifyLdr(noContact{f.noContact, err})
	} else {
		f.noContact = time.Time{} // zeroing
		debug(f, "yesContact")
		f.notifyLdr(noContact{f.noContact, nil})
	}
}

func (f *flr) getEntryTerm(i uint64) (uint64, error) {
	b, err := f.log.Get(i)
	if err == errNoEntryFound {
		return 0, err
	} else if err != nil {
		panic(opError(err, "Log.Get(%d)", i))
	}
	e := &entry{}
	if err = e.decode(bytes.NewReader(b)); err != nil {
		panic(opError(err, "log.Get(%d).decode()", i))
	}
	return e.term, nil
}

func (f *flr) writeEntriesTo(c *conn, from uint64, n uint64) error {
	buffs, err := f.log.GetN(from, n)
	if err != nil {
		panic(opError(err, "Log.GetN(%d, %d)", from, n))
	}
	nbuffs := net.Buffers(buffs)
	// todo: set write deadline based on size
	_, err = nbuffs.WriteTo(c.rwc)
	return err
}

// ------------------------------------------------

type leaderUpdate struct {
	log         *log.Log
	commitIndex uint64
	config      *Config // nil if config not changed
}

type replUpdate struct {
	status *flrStatus
	update interface{}
}

type matchIndex struct {
	val uint64
}

type noContact struct {
	time time.Time
	err  error
}

type removeLTE struct {
	val uint64
}

type newTerm struct {
	val uint64
}

type flrStatus struct {
	id uint64

	// true when the flr is removed
	// used to ignore replUpdate from removed flr
	removed bool

	// owned exclusively by leader goroutine
	// used to compute majorityMatchIndex
	matchIndex uint64

	// from what time the replication unable to reach this node
	// zero value means it is reachable
	noContact time.Time

	err error

	round *Round // nil if no promotion required

	removeLTE uint64
}
