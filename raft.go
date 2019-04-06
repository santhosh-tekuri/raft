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
	"net"
	"runtime"
	"sync"
	"time"
)

// when running tests this is set to true
var testMode bool

type Raft struct {
	rtime randTime
	timer *safeTimer
	rpcCh chan *rpc

	fsm           *stateMachine
	fsmRestoredCh chan error // fsm reports any errors during restore on this channel
	snapTimer     *safeTimer
	snapInterval  time.Duration
	snapThreshold uint64
	snapTakenCh   chan snapTaken // non nil only when snapshot task is in progress

	// persistent state
	*storage

	// volatile state
	state       State
	leader      uint64
	commitIndex uint64

	// options
	hbTimeout        time.Duration
	quorumWait       time.Duration
	promoteThreshold time.Duration
	shutdownOnRemove bool
	trace            Trace
	bandwidth        int64

	// dialing
	resolver  *resolver
	dialFn    dialFn // used for mocking in tests
	connPools map[uint64]*connPool

	ldr *leader
	cnd *candidate

	taskCh    chan Task
	fsmTaskCh chan FSMTask

	closeOnce sync.Once
	close     chan struct{}
	closed    chan struct{}
}

func New(opt Options, fsm FSM, storage *Storage) (*Raft, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	store := storage.storage
	if store.cid == 0 || store.nid == 0 {
		return nil, ErrIdentityNotSet
	}
	sm := &stateMachine{
		FSM:   fsm,
		id:    store.nid,
		ch:    make(chan interface{}, 1024), // todo configurable capacity
		snaps: store.snaps,
	}

	r := &Raft{
		rtime:            newRandTime(),
		timer:            newSafeTimer(),
		rpcCh:            make(chan *rpc),
		fsm:              sm,
		fsmRestoredCh:    make(chan error, 5),
		snapTimer:        newSafeTimer(),
		snapInterval:     opt.SnapshotInterval,
		snapThreshold:    opt.SnapshotThreshold,
		storage:          store,
		state:            Follower,
		hbTimeout:        opt.HeartbeatTimeout,
		quorumWait:       opt.QuorumWait,
		promoteThreshold: opt.PromoteThreshold,
		shutdownOnRemove: opt.ShutdownOnRemove,
		trace:            opt.Trace,
		bandwidth:        opt.Bandwidth,
		dialFn:           net.DialTimeout,
		connPools:        make(map[uint64]*connPool),
		taskCh:           make(chan Task),
		fsmTaskCh:        make(chan FSMTask, maxAppendEntries), // todo
		close:            make(chan struct{}),
		closed:           make(chan struct{}),
	}

	r.resolver = &resolver{
		delegate: opt.Resolver,
		addrs:    make(map[uint64]string),
		trace:    &r.trace,
	}
	r.resolver.update(store.configs.Latest)

	return r, nil
}

// todo: note that we dont support multiple listeners

func (r *Raft) Serve(l net.Listener) error {
	defer close(r.closed)

	if r.isClosed() {
		return ErrServerClosed
	}
	if trace {
		println(r, "serving at", l.Addr())
		defer println(r, "<< shutdown()")
	}
	if r.trace.Starting != nil {
		r.trace.Starting(r.liveInfo(), l.Addr())
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.fsm.runLoop()
		if trace {
			println(r, "fsmLoop shutdown")
		}
	}()
	defer close(r.fsm.ch)

	// restore fsm from last snapshot, if present
	if r.snaps.index > 0 {
		r.fsm.ch <- fsmRestoreReq{r.fsmRestoredCh}
		if err := <-r.fsmRestoredCh; err != nil {
			return err
		}
		r.commitIndex = r.snaps.index
	}

	s := newServer(l)
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.serve(r.rpcCh)
		if trace {
			println(r, "server shutdown")
		}
	}()
	defer s.shutdown()

	return r.stateLoop()
}

func (r *Raft) stateLoop() (err error) {
	var (
		f = &follower{Raft: r}
		c = &candidate{Raft: r}
		l = &leader{
			Raft:  r,
			repls: make(map[uint64]*replication),
			transfer: transfer{
				timer:        newSafeTimer(),
				newTermTimer: newSafeTimer(),
			},
		}
	)
	r.ldr, r.cnd = l, c

	states := map[State]interface {
		init()
		release()
		onTimeout()
	}{
		Follower:  f,
		Candidate: c,
		Leader:    l,
	}

	defer func() {
		if v := recover(); v != nil {
			if _, ok := v.(runtime.Error); ok {
				panic(v)
			}
			err = toErr(v)
			r.doClose(err)
		}
		if trace {
			println(r, "stateLoop shutdown")
		}
	}()

	var state State
	defer func() {
		states[state].release()
		r.release()
	}()
	if r.snapInterval > 0 {
		r.snapTimer.reset(r.rtime.duration(r.snapInterval))
	}
	for {
		state = r.state
		states[state].init()
		for r.state == state {
			select {
			case <-r.close:
				return ErrServerClosed

			case err := <-r.fsmRestoredCh:
				if trace {
					println(r, "fsm restored err", err)
				}
				if err != nil {
					panic(err)
				}

			case <-r.snapTimer.C:
				r.snapTimer.active = false
				r.onTakeSnapshot(takeSnapshot{threshold: r.snapThreshold})

			case rpc := <-r.rpcCh:
				resetTimer := r.replyRPC(rpc)
				// on receiving AppendEntries from current leader or
				// granting vote to candidate reset timer
				if r.state == Follower && resetTimer {
					f.resetTimer()
				}

			case <-r.timer.C:
				r.timer.active = false
				states[r.state].onTimeout()

			case t := <-r.fsmTaskCh:
				ne := t.newEntry()
				if r.state == Leader {
					l.storeEntry(ne)
				} else {
					ne.reply(NotLeaderError{r.leader, r.leaderAddr(), false})
				}

			case t := <-r.taskCh:
				f.executeTask(t)
				if r.state == Follower && f.electionAborted {
					f.resetTimer()
				}

			case t := <-r.snapTakenCh:
				r.onSnapshotTaken(t)
				if r.snapInterval > 0 {
					r.snapTimer.reset(r.rtime.duration(r.snapInterval))
				}

			// candidate --------------
			case v := <-c.respCh:
				assert(r.state == Candidate)
				c.onVoteResult(v)

			// leader --------------
			case u := <-l.replUpdateCh:
				assert(r.state == Leader)
				l.checkReplUpdates(u)

			case <-l.transfer.timer.C:
				l.transfer.timer.active = false
				l.onTransferTimeout()

			case result := <-l.transfer.respCh:
				l.onTimeoutNowResult(result)

			case <-l.transfer.newTermTimer.C:
				l.transfer.newTermTimer.active = false
				l.onNewTermTimeout()
			}
		}
		r.timer.stop()
		states[state].release()
	}
}

// closing -----------------------------------------

func (r *Raft) release() {
	// close any open connections
	for _, pool := range r.connPools {
		pool.closeAll()
	}

	// wait for snapshot to complete
	if r.snapTakenCh != nil {
		r.onSnapshotTaken(<-r.snapTakenCh)
	}
}

func (r *Raft) doClose(reason error) {
	r.closeOnce.Do(func() {
		if trace {
			println(r, ">> shutdown()", reason)
		}
		if r.trace.ShuttingDown != nil {
			r.trace.ShuttingDown(r.liveInfo(), reason)
		}
		close(r.close)
		for buffed := len(r.fsmTaskCh); buffed > 0; buffed-- {
			t := <-r.fsmTaskCh
			t.reply(ErrServerClosed)
		}
	})
}

func (r *Raft) Shutdown() <-chan struct{} {
	r.doClose(ErrServerClosed)
	return r.closed
}

func (r *Raft) Closed() <-chan struct{} {
	return r.close
}

func (r *Raft) isClosed() bool {
	return isClosed(r.close)
}

// misc --------------------------------------------------------

func (r *Raft) setState(s State) {
	if s != r.state {
		if trace {
			println(r, r.state, "->", s)
		}
		r.state = s
		if r.trace.StateChanged != nil {
			r.trace.StateChanged(r.liveInfo())
		}
	}
}

func (r *Raft) setLeader(id uint64) {
	if id != r.leader {
		if trace {
			println(r, "leader:", id)
		}
		r.leader = id
		if r.trace.LeaderChanged != nil {
			r.trace.LeaderChanged(r.liveInfo())
		}
	}
}

// CID returns cluster ID.
func (r *Raft) CID() uint64 {
	return r.cid
}

// NID returns node ID.
func (r *Raft) NID() uint64 {
	return r.nid
}

func (r *Raft) FSM() FSM {
	return r.fsm.FSM
}

func (r *Raft) addr() string {
	addr, _ := r.resolver.lookupID(r.nid, 10*time.Second)
	return addr
}

func (r *Raft) leaderAddr() string {
	if r.leader == 0 {
		return ""
	}
	addr, _ := r.resolver.lookupID(r.leader, 10*time.Second)
	return addr
}

func (r *Raft) liveInfo() Info {
	return liveInfo{r: r}
}

// state ----------------------------------

type State byte

const (
	Follower  State = 'F'
	Candidate       = 'C'
	Leader          = 'L'
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return string(s)
}
