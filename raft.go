package raft

import (
	"container/list"
	"net"
	"sync"
	"time"
)

type Raft struct {
	rtime randTime
	timer *safeTimer
	rpcCh chan *rpc

	fsm           *stateMachine
	snapTakenCh   chan snapTaken // non nil only when snapshot task is in progress
	snapThreshold uint64

	// persistent state
	*storage

	// volatile state
	state       State
	leader      uint64
	commitIndex uint64
	lastApplied uint64

	// options
	hbTimeout        time.Duration
	quorumWait       time.Duration
	promoteThreshold time.Duration
	trace            Trace

	// dialing flrs
	resolver  *resolver
	dialFn    dialFn // used for mocking in tests
	connPools map[uint64]*connPool

	ldr       *ldrShip
	taskCh    chan Task
	fsmTaskCh chan FSMTask

	closeOnce sync.Once
	close     chan struct{}
	closed    chan struct{}
}

func New(opt Options, fsm FSM, storage Storage) (*Raft, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	store := newStorage(storage)
	if err := store.init(); err != nil {
		return nil, err
	}
	sm := &stateMachine{
		FSM:       fsm,
		id:        store.nid,
		taskCh:    make(chan Task, 128), // todo configurable capacity
		snapshots: storage.Snapshots,
	}

	r := &Raft{
		rtime:            newRandTime(),
		timer:            newSafeTimer(),
		rpcCh:            make(chan *rpc),
		fsm:              sm,
		snapThreshold:    opt.SnapshotThreshold,
		storage:          store,
		state:            Follower,
		hbTimeout:        opt.HeartbeatTimeout,
		quorumWait:       opt.QuorumWait,
		promoteThreshold: opt.PromoteThreshold,
		trace:            opt.Trace,
		dialFn:           net.DialTimeout,
		connPools:        make(map[uint64]*connPool),
		taskCh:           make(chan Task),
		fsmTaskCh:        make(chan FSMTask),
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

	if r.isClosing() {
		return ErrServerClosed
	}
	debug(r, "starting....")
	if r.trace.Starting != nil {
		r.trace.Starting(r.liveInfo())
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.fsm.runLoop()
		debug(r.nid, "fsmLoop shutdown")
	}()
	defer close(r.fsm.taskCh)

	// restore fsm from last snapshot, if present
	if r.snapIndex > 0 {
		if err := r.restoreFSM(); err != nil {
			close(r.fsm.taskCh) // to stop fsmLoop
			return err
		}
	}

	s := newServer(l)
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.serve(r.rpcCh)
		debug(r.nid, "server shutdown")
	}()
	defer s.shutdown()

	return r.stateLoop()
}

func (r *Raft) stateLoop() (err error) {
	var (
		f = &flrShip{Raft: r}
		c = &candShip{Raft: r}
		l = &ldrShip{
			Raft:       r,
			newEntries: list.New(),
			flrs:       make(map[uint64]*flr),
			transfer:   transfer{timer: newSafeTimer()},
		}
	)
	r.ldr = l
	ships := map[State]stateShip{
		Follower:  f,
		Candidate: c,
		Leader:    l,
	}

	defer func() {
		if v := recover(); v != nil {
			err = toErr(v)
			r.doClose(err)
		}
		debug(r, "stateLoop shutdown")
	}()

	var rstate State
	defer func() {
		ships[rstate].release()
		r.release()
	}()

	for {
		rstate = r.state
		ships[rstate].init()
		for r.state == rstate {
			select {
			case <-r.close:
				return ErrServerClosed

			case rpc := <-r.rpcCh:
				resetTimer := r.replyRPC(rpc)
				// on receiving AppendEntries from current leader or
				// granting vote to candidate reset timer
				if r.state == Follower && resetTimer {
					f.resetTimer()
				}

			case <-r.timer.C:
				r.timer.active = false
				ships[r.state].onTimeout()

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

			// candidate --------------
			case v := <-c.respCh:
				assert(r.state == Candidate, "M%d BUG: %v", r.nid, r.state)
				c.onVoteResult(v)

			// leader --------------
			case u := <-l.fromReplsCh:
				assert(r.state == Leader, "M%d BUG: %v", r.nid, r.state)
				l.checkReplUpdates(u)

			case <-l.transfer.timer.C:
				l.transfer.timer.active = false
				l.onTransferTimeout()

			case result := <-l.transfer.respCh:
				l.onTimeoutNowResult(result)
			}
		}
		r.timer.stop()
		ships[rstate].release()
	}
}

// closing -----------------------------------------

func (r *Raft) release() {
	// wait for snapshot to complete
	if r.snapTakenCh != nil {
		r.onSnapshotTaken(<-r.snapTakenCh)
	}

	// close any open connections
	for _, pool := range r.connPools {
		pool.closeAll()
	}
}

func (r *Raft) doClose(reason error) {
	r.closeOnce.Do(func() {
		debug(r.nid, ">> shutdown()", reason)
		if r.trace.ShuttingDown != nil {
			r.trace.ShuttingDown(r.liveInfo(), reason)
		}
		close(r.close)
	})
}

func (r *Raft) Shutdown() <-chan struct{} {
	r.doClose(ErrServerClosed)
	return r.closed
}

func (r *Raft) Closing() <-chan struct{} {
	return r.close
}

// tells whether shutdown was called
func (r *Raft) isClosing() bool {
	return isClosed(r.close)
}

// misc --------------------------------------------------------

func (r *Raft) setState(s State) {
	if s != r.state {
		debug(r, r.state, "->", s)
		if r.trace.StateChanged != nil {
			r.trace.StateChanged(r.liveInfo())
		}
	}
	r.state = s
}

func (r *Raft) setLeader(id uint64) {
	if id != r.leader {
		debug(r, "leader:", id)
		if r.trace.LeaderChanged != nil {
			r.trace.LeaderChanged(r.liveInfo())
		}
	}
	r.leader = id
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
	addr, _ := r.resolver.lookupID(r.nid)
	return addr
}

func (r *Raft) leaderAddr() string {
	if r.leader == 0 {
		return ""
	}
	addr, _ := r.resolver.lookupID(r.leader)
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

type stateShip interface {
	init()
	release()
	onTimeout()
}
