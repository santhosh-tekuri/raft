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
	"context"
	"net"
	"path/filepath"
	"sync"
	"time"
)

// when running tests this is set to true
var testMode bool

// Raft implements raft node.
type Raft struct {
	rtime randTime
	timer *safeTimer

	rpcCh        chan *rpc
	disconnected chan uint64 // nid

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
	logger           Logger
	alerts           Alerts
	bandwidth        int64

	// dialing
	resolver  *resolver
	dialFn    dialFn // used for mocking in tests
	connPools map[uint64]*connPool

	ldr *leader
	cnd *candidate

	taskCh     chan Task
	fsmTaskCh  chan FSMTask
	newEntryCh chan *newEntry

	closeOnce   sync.Once
	closeReason error
	close       chan struct{}
	closed      chan struct{}
}

// New is used to construct a new Raft node.
// If storageDir already contains lock file, it returns ErrLockExists.
// If identity is not set in storageDir, it returns ErrIdentityNotSet.
func New(opt Options, fsm FSM, storageDir string) (*Raft, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	if opt.Logger == nil {
		opt.Logger = nopLogger{}
	}
	if opt.Alerts == nil {
		opt.Alerts = nopAlerts{}
	}
	store, err := openStorage(storageDir, opt)
	if err != nil {
		return nil, err
	}
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
		disconnected:     make(chan uint64, 20),
		fsm:              sm,
		fsmRestoredCh:    make(chan error, 5),
		snapTimer:        newSafeTimer(),
		snapInterval:     opt.SnapshotInterval,
		snapThreshold:    opt.SnapshotThreshold,
		storage:          store,
		state:            Follower,
		hbTimeout:        opt.HeartbeatTimeout,
		promoteThreshold: opt.PromoteThreshold,
		shutdownOnRemove: opt.ShutdownOnRemove,
		logger:           opt.Logger,
		alerts:           opt.Alerts,
		bandwidth:        opt.Bandwidth,
		dialFn:           net.DialTimeout,
		connPools:        make(map[uint64]*connPool),
		taskCh:           make(chan Task),
		fsmTaskCh:        make(chan FSMTask),
		newEntryCh:       make(chan *newEntry),
		close:            make(chan struct{}),
		closed:           make(chan struct{}),
	}

	r.resolver = &resolver{
		delegate: opt.Resolver,
		addrs:    make(map[uint64]string),
		logger:   r.logger,
		alerts:   r.alerts,
	}
	r.resolver.update(store.configs.Latest)

	return r, nil
}

// todo: note that we dont support multiple listeners

// ListenAndServe listens on the TCP network address addr and
// then calls Serve.
//
// ListenAndServe always returns a non-nil error. If raft is
// closed by Shutdown call, it returns ErrServerClosed. If
// raft is closed because it is removed from cluster, it returns
// ErrNodeRemoved. If there is any error with storage or FSM, it
// returns OpError.
//
// Note that the address specified here could be different than
// the address specified in config. The address specified in config
// is the advertised address, which should be reachable from other
// nodes in the cluster.
func (r *Raft) ListenAndServe(addr string) error {
	lr, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	return r.Serve(lr)
}

// Serve accepts incoming connections from raft nodes on the listener.
//
// Serve always returns a non-nil error. If raft is
// closed by Shutdown call, it returns ErrServerClosed. If
// raft is closed because it is removed from cluster, it returns
// ErrNodeRemoved. If there is any error with storage or FSM, it
//// returns OpError.
//
// Note that the address specified here could be different than
// the address specified in config. The address specified in config
// is the advertised address, which should be reachable from other
// nodes in the cluster.
func (r *Raft) Serve(l net.Listener) error {
	defer safeClose(r.closed)
	if r.isClosed() {
		return ErrServerClosed
	}
	storageDir := filepath.Dir(r.snaps.dir)
	if err := lockDir(storageDir); err != nil {
		return err
	}
	defer unlockDir(storageDir)
	if trace {
		println(r, "serving at", l.Addr())
		defer println(r, "<< shutdown()")
	}
	r.logger.Info("storage:", filepath.Dir(r.snaps.dir))
	r.logger.Info("cid:", r.cid, "nid:", r.nid)
	r.logger.Info(r.configs.Latest)
	r.logger.Info("listening at", l.Addr())

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

	s := newServer(r, l)
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.serve()
		if trace {
			println(r, "server shutdown")
		}
	}()
	defer s.shutdown()

	go r.runBatch()
	r.stateLoop()
	for ne := range r.newEntryCh {
		for ne != nil {
			ne.reply(ErrServerClosed)
			ne = ne.next
		}
	}
	return r.closeReason
}

func (r *Raft) stateLoop() {
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
			r.doClose(recoverErr(v))
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
				return

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

			case nid := <-r.disconnected:
				if r.leader != 0 && nid != 0 && r.leader == nid {
					if trace {
						println(r, "leader got disconnected")
					}
					r.setLeader(0)
				}

			case <-r.timer.C:
				r.timer.active = false
				states[r.state].onTimeout()

			case ne, ok := <-r.newEntryCh:
				if ok {
					if r.state == Leader {
						l.storeEntry(ne)
					} else {
						for ne != nil {
							if ne.typ == entryDirtyRead {
								r.fsm.ch <- fsmDirtyRead{ne}
							} else {
								ne.reply(notLeaderError(r, false))
							}
							ne = ne.next
						}
					}
				} else {
					assert(r.isClosed())
					return
				}

			case t := <-r.taskCh:
				r.executeTask(t)
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
				c.onVoteResult(v)

			// leader --------------
			case u := <-l.replUpdateCh:
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
		r.closeReason = reason
		if trace {
			println(r, ">> shutdown()", reason)
		}
		if reason == ErrServerClosed {
			r.logger.Info("shutting down")
		} else if reason == ErrNodeRemoved {
			r.logger.Info("node removed, shutting down")
		} else {
			r.logger.Warn(trimPrefix(reason), "shutting down")
		}
		r.alerts.ShuttingDown(reason)
		if tracer.shuttingDown != nil {
			tracer.shuttingDown(r, reason)
		}
		close(r.close)
	})
}

// Shutdown gracefully shuts down the server. If the provided context expires before
// the shutdown is complete, Shutdown returns the context's error, otherwise it returns nil
func (r *Raft) Shutdown(ctx context.Context) error {
	r.doClose(ErrServerClosed)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.closed:
		return nil
	}
}

// Closed returns a channel which is closed when the raft
// initiated shutdown process. You should check this before
// submitting any task as shown below:
//
//     t := raft.GetInfo()
//     select {
//         case <-r.Closed():
//             return nil, ErrServerClosed
//         case r.Tasks() <-t:
//     }
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
		r.logger.Info("changing state", r.state, "->", s)
		r.state = s
		if tracer.stateChanged != nil {
			tracer.stateChanged(r)
		}
	}
}

func (r *Raft) setLeader(id uint64) {
	if id != r.leader {
		if trace {
			println(r, "leader:", id)
		}
		r.leader = id
		if r.leader == 0 {
			r.logger.Info("no known leader")
		} else if r.leader == r.nid {
			r.logger.Info("cluster leadership acquired")
		} else {
			r.logger.Info("following leader node", r.leader)
		}
		if tracer.leaderChanged != nil {
			tracer.leaderChanged(r)
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

// FSM return the FSM.
func (r *Raft) FSM() FSM {
	return r.fsm.FSM
}

func (r *Raft) addr() string {
	return r.resolver.lookupID(r.nid, 10*time.Second)
}

func (r *Raft) leaderAddr() string {
	if r.leader == 0 {
		return ""
	}
	return r.resolver.lookupID(r.leader, 10*time.Second)
}

// state ----------------------------------

// State captures the state of a Raft node: Follower, Candidate, Leader.
type State byte

// Possible states of a raft node.
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
