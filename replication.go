// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

type replication struct {
	rtime  randTime
	status replicationStatus // owned by ldr goroutine

	connPool  *connPool
	log       *log.Log
	snaps     *snapshots
	hbTimeout time.Duration
	timer     *safeTimer
	bandwidth int64

	ldrStartIndex uint64
	ldrLastIndex  uint64 // todo: directly use log.lastIndex
	matchIndex    uint64
	nextIndex     uint64

	node Node

	// from this time node is unreachable
	// zero value means node is reachable
	noContact time.Time

	leaderUpdateCh chan leaderUpdate
	replUpdateCh   chan<- replUpdate
	stopCh         chan struct{}
}

func (r *replication) runLoop(req *appendReq) {
	if trace {
		println(r, "repl.start")
	}
	var c *conn
	defer func() {
		if c != nil && c.rwc != nil {
			r.connPool.returnConn(c)
		}
		if v := recover(); v != nil {
			if _, ok := v.(runtime.Error); ok {
				panic(v)
			}
			r.notifyLdr(toErr(v))
		}
	}()

	failures, err := uint64(0), error(nil)
	for {
		if failures > 0 {
			if failures == 1 {
				r.notifyNoContact(err)
			}
			r.timer.reset(backOff(failures, r.hbTimeout/2))
			select {
			case <-r.stopCh:
				return
			case <-r.timer.C:
				r.timer.active = false
			}
			if _, err = r.checkLeaderUpdate(r.stopCh, req, false); err == errStop {
				return
			}
		}

		if c == nil {
			if c, err = r.connPool.getConn(r.deadline()); err != nil {
				failures++
				continue
			}
			if failures > 0 {
				failures = 0
				r.notifyNoContact(nil)
				if _, err = r.checkLeaderUpdate(r.stopCh, req, false); err == errStop {
					return
				}
			}
		}

		assert(r.matchIndex < r.nextIndex)
		err = r.replicate(c, req)
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
func (r *replication) replicate(c *conn, req *appendReq) error {
	resp := &appendResp{}
	for {
		// find matchIndex ---------------------------------------------------
		for {
			err := r.writeAppendEntriesReq(c, req, false)
			if err == log.ErrNotFound {
				if err = r.sendInstallSnapReq(c, req); err == nil {
					continue
				}
			}
			if err != nil {
				return err
			}

			if err = c.readResp(resp, r.deadline()); err != nil {
				return err
			}
			if err = r.onAppendEntriesResp(resp, r.nextIndex-1); err != nil {
				return err
			}
			if _, err = r.checkLeaderUpdate(r.stopCh, req, false); err != nil {
				return err
			}
			if r.matchIndex+1 == r.nextIndex {
				break
			}
		}

		if r.nextIndex < r.ldrLastIndex && !r.log.Contains(r.nextIndex) {
			if err := r.sendInstallSnapReq(c, req); err == nil {
				continue
			}
		}

		// todo: before starting pipeline, check if sending snap
		//       is better than sending lots of entries

		if trace {
			println(r, "pipelining.............................")
		}
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
				err := r.writeAppendEntriesReq(c, req, true)
				select {
				case <-stopCh:
					return
				case resultCh <- result{r.nextIndex - 1, err}:
				}
				if err != nil {
					return
				}
				for {
					ldrUpdate, errStop := r.checkLeaderUpdate(stopCh, req, true)
					if errStop != nil {
						return
					}
					if ldrUpdate || len(resultCh) == 0 || r.nextIndex <= r.ldrLastIndex {
						break
					}
					// we have not received responses for all requests yet
					// so no need to flood him with heartbeat
					// take a nap and check again
					if trace {
						println(r, "no heartbeat, pending resps:", len(resultCh))
					}
				}
			}
		}()

		drainResps := func() error {
			for range resultCh {
				if err := c.readResp(resp, r.deadline()); err != nil {
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
				if trace {
					println(r, "drain completed with error:", err)
				}
				if err != nil {
					_ = c.rwc.Close()
					c.rwc = nil // to signal runLoop that we closed the conn
				}
			case <-time.After(timeout):
				if trace {
					println(r, "drain timeout, closing conn")
				}
				_ = c.rwc.Close()
				<-drained
				if trace {
					println(r, "drain completed")
				}
				c.rwc = nil // to signal runLoop that we closed the conn
			}
		}

		var err error
		for {
			var result result
			select {
			case <-r.stopCh:
				if trace {
					println(r, "ending pipeline, got stop signal from ldr")
				}
				close(stopCh)
				drainRespsTimeout(r.hbTimeout / 2)
				return errStop
			case result = <-resultCh:
			}
			if result.err != nil {
				if trace {
					println(r, "pipeline ended with", result.err)
				}
				if result.err == log.ErrNotFound {
					break
				}
				return result.err
			}
			err = c.readResp(resp, r.deadline())
			if err != nil {
				if trace {
					println(r, "ending pipeline, error in reading resp:", err)
				}
				close(stopCh)
				_ = c.rwc.Close()
				for range resultCh {
				}
				c.rwc = nil
				return err
			}
			if resp.result == success {
				_ = r.onAppendEntriesResp(resp, result.lastIndex)
			} else {
				if trace {
					println(r, "ending pipeline, got resp.result", resp.result)
				}
				close(stopCh)
				if resp.result == staleTerm {
					drainRespsTimeout(r.hbTimeout / 2)
					return r.onAppendEntriesResp(resp, result.lastIndex) // notifies ldr and return errStop
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
func (r *replication) writeAppendEntriesReq(c *conn, req *appendReq, sendEntries bool) error {
	snapIndex, snapTerm := r.snaps.latest()

	// fill req.prevLogXXX
	req.prevLogIndex = r.nextIndex - 1
	if req.prevLogIndex == 0 {
		req.prevLogTerm = 0
	} else if req.prevLogIndex == snapIndex {
		req.prevLogTerm = snapTerm
	} else {
		term, err := r.getEntryTerm(req.prevLogIndex)
		if err != nil { // should be in snapshot
			return err
		}
		req.prevLogTerm = term
	}

	req.numEntries = 0
	if sendEntries {
		req.numEntries = min(r.ldrLastIndex-req.prevLogIndex, maxAppendEntries)
		if req.numEntries > 0 && !r.log.Contains(r.nextIndex) {
			return log.ErrNotFound
		}
	}

	if trace {
		if sendEntries && req.numEntries == 0 {
			println(r, ">> heartbeat")
		} else {
			println(r, ">>", req)
		}
	}
	if err := c.writeReq(req, r.deadline()); err != nil {
		return err
	}
	if req.numEntries > 0 {
		if err := r.writeEntriesTo(c, r.nextIndex, req.numEntries); err != nil {
			return err
		}
		r.nextIndex += req.numEntries
		if trace {
			println(r, "nextIndex:", r.nextIndex)
		}
	}
	return nil
}

func (r *replication) onAppendEntriesResp(resp *appendResp, reqLastIndex uint64) error {
	if trace {
		println(r, "<<", resp)
	}
	switch resp.result {
	case staleTerm:
		r.notifyLdr(newTerm{resp.getTerm()})
		return errStop
	case success:
		if reqLastIndex > r.matchIndex {
			r.matchIndex = reqLastIndex
			if trace {
				println(r, "matchIndex:", r.matchIndex)
			}
			r.notifyLdr(matchIndex{r.matchIndex})
		}
		return nil
	case prevEntryNotFound, prevTermMismatch:
		if resp.lastLogIndex < r.matchIndex {
			// this happens if someone restarted follower storage with empty storage
			return ErrFaultyFollower
		}
		r.nextIndex = min(r.nextIndex-1, resp.lastLogIndex+1)
		if trace {
			println(r, "nextIndex:", r.nextIndex)
		}
		return nil
	case unexpectedErr:
		return remoteError{resp.err}
	default:
		panic(fmt.Errorf("[BUG] appendEntries.result==%v", resp.result))
	}
}

func (r *replication) sendInstallSnapReq(c *conn, appReq *appendReq) error {
	snap, err := r.snaps.open()
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
	if trace {
		println(r, ">>", req)
	}
	if err = c.writeReq(req, r.deadline()); err != nil {
		return err
	}
	if err := c.rwc.SetWriteDeadline(r.deadlineSize(req.size)); err != nil {
		return err
	}
	if _, err = io.Copy(c.rwc, snap.file); err != nil { // will use sendFile
		return err
	}

	resp := &installSnapResp{}
	if err = c.readResp(resp, time.Now().Add(4*r.hbTimeout)); err != nil { // todo: is 2*hbTimeout enough for saving snap
		return err
	}
	switch resp.result {
	case staleTerm:
		r.notifyLdr(newTerm{resp.getTerm()})
		return errStop
	case success:
		// case: snapshot was taken before we got leaderUpdate about lastLogIndex
		// we should wait until we get our logview gets updated
		for req.lastIndex > r.ldrLastIndex {
			if _, err := r.checkLeaderUpdate(r.stopCh, appReq, false); err != nil {
				return err
			}
		}
		r.matchIndex = req.lastIndex
		r.nextIndex = r.matchIndex + 1
		if trace {
			println(r, "matchIndex:", r.matchIndex, "nextIndex:", r.nextIndex)
		}
		r.notifyLdr(matchIndex{r.matchIndex})
		return nil
	case unexpectedErr:
		return remoteError{resp.err}
	default:
		panic(fmt.Errorf("[BUG] installSnapResp.result==%v", resp.result))
	}
}

func (r *replication) checkLeaderUpdate(stopCh <-chan struct{}, req *appendReq, sendEntries bool) (ldrUpdate bool, err error) {
	if sendEntries && r.nextIndex > r.ldrLastIndex {
		// for nonvoter, dont send heartbeats
		var timerCh <-chan time.Time
		if r.node.Voter {
			r.timer.reset(r.hbTimeout / 2)
			timerCh = r.timer.C
		}

		// nothing to send. start heartbeat timer
		select {
		case <-stopCh:
			return false, errStop
		case update := <-r.leaderUpdateCh:
			r.onLeaderUpdate(update, req)
			return true, nil
		case <-timerCh:
			r.timer.active = false
			return false, nil
		}
	} else {
		// check signal if any, without blocking
		select {
		case <-stopCh:
			return false, errStop
		case update := <-r.leaderUpdateCh:
			r.onLeaderUpdate(update, req)
			return true, nil
		default:
		}
	}
	return false, nil
}

func (r *replication) onLeaderUpdate(u leaderUpdate, req *appendReq) {
	if trace {
		println(r, u)
	}
	if u.log.PrevIndex() > r.log.PrevIndex() {
		r.notifyLdr(removeLTE{u.log.PrevIndex()})
	}
	r.log = u.log
	r.ldrLastIndex, req.ldrCommitIndex = u.log.LastIndex(), u.commitIndex
	if u.config != nil {
		r.node = u.config.Nodes[r.status.id]
	}
}

func (r *replication) notifyLdr(u interface{}) {
	select {
	case <-r.stopCh:
	case r.replUpdateCh <- replUpdate{&r.status, u}:
	}
}

func (r *replication) notifyNoContact(err error) {
	if err != nil {
		r.noContact = time.Now()
		if trace {
			println(r, "noContact", err)
		}
		r.notifyLdr(noContact{r.noContact, err})
	} else {
		r.noContact = time.Time{} // zeroing
		if trace {
			println(r, "yesContact")
		}
		r.notifyLdr(noContact{r.noContact, nil})
	}
}

func (r *replication) getEntryTerm(i uint64) (uint64, error) {
	b, err := r.log.Get(i)
	if err == log.ErrNotFound {
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

func (r *replication) writeEntriesTo(c *conn, from uint64, n uint64) error {
	buffs, err := r.log.GetN(from, n)
	if err != nil {
		panic(opError(err, "Log.GetN(%d, %d)", from, n))
	}
	nbuffs := net.Buffers(buffs)
	if err := c.rwc.SetWriteDeadline(r.deadlineSize(size(nbuffs))); err != nil {
		return err
	}
	_, err = nbuffs.WriteTo(c.rwc)
	return err
}

func (r *replication) deadline() time.Time {
	return time.Now().Add(2 * r.hbTimeout)
}

func (r *replication) deadlineSize(size int64) time.Time {
	timeout := durationFor(r.bandwidth, size)
	if timeout < 2*r.hbTimeout {
		timeout = 2 * r.hbTimeout
	}
	return time.Now().Add(timeout)
}

// ------------------------------------------------

type leaderUpdate struct {
	log         *log.Log
	commitIndex uint64
	config      *Config // nil if config not changed
}

type replUpdate struct {
	status *replicationStatus
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

type replicationStatus struct {
	id uint64

	// true when the replication is removed
	// used to ignore replUpdate from removed replication
	removed bool

	// owned exclusively by leader goroutine
	// used to compute majorityMatchIndex
	matchIndex uint64

	// from what time the replication unable to reach this node
	// zero value means it is reachable
	noContact time.Time

	err error

	node Node

	round *Round // nil if no promotion required

	removeLTE uint64
}
