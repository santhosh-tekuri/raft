package raft

import (
	"container/list"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Raft struct {
	id     uint64
	rtime  randTime
	timer  *safeTimer
	server *server

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

	ldr        *ldrShip
	taskCh     chan Task
	newEntryCh chan NewEntry

	appendErr error

	wg         sync.WaitGroup
	shutdownMu sync.Mutex
	shutdownCh chan struct{}
}

func New(id uint64, opt Options, fsm FSM, storage Storage) (*Raft, error) {
	if id == 0 {
		return nil, errors.New("raft: id must be greater than zero")
	}
	if err := opt.validate(); err != nil {
		return nil, err
	}
	store := newStorage(storage)
	if err := store.init(); err != nil {
		return nil, err
	}
	sm := &stateMachine{
		FSM:       fsm,
		id:        id,
		taskCh:    make(chan Task, 128), // todo configurable capacity
		snapshots: storage.Snapshots,
	}

	r := &Raft{
		id:               id,
		rtime:            newRandTime(),
		timer:            newSafeTimer(),
		server:           newServer(),
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
		newEntryCh:       make(chan NewEntry),
		shutdownCh:       make(chan struct{}),
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
	// ensure waitGroup FirstAdd and Wait order
	r.shutdownMu.Lock()
	closed := r.isClosed()
	if !closed {
		r.wg.Add(1)
	}
	r.shutdownMu.Unlock()

	if closed {
		return ErrServerClosed
	}

	debug(r, "starting....")
	if r.trace.Starting != nil {
		r.trace.Starting(r.liveInfo())
	}

	var wg sync.WaitGroup
	defer func() {
		debug(r, "stateLoop shutdown")
		r.server.shutdown()
		debug(r, "server shutdown")
		close(r.fsm.taskCh)
		wg.Wait()
		r.wg.Done()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.fsm.runLoop()
		debug(r.id, "fsmLoop shutdown")
	}()

	// restore fsm from last snapshot, if present
	if r.snapIndex > 0 {
		if err := r.restoreFSMFromSnapshot(); err != nil {
			close(r.fsm.taskCh) // to stop fsmLoop
			return err
		}
	}

	go r.server.serve(l)
	return r.stateLoop()
}

func (r *Raft) stateLoop() (err error) {
	var (
		f = &flrShip{Raft: r}
		c = &candShip{Raft: r}
		l = &ldrShip{
			Raft:          r,
			newEntries:    list.New(),
			flrs:          make(map[uint64]*flr),
			transferTimer: newSafeTimer(),
			transferLdr:   TransferLeadership(0).(transferLdr),
		}
	)
	r.ldr = l
	ships := map[State]stateShip{
		Follower:  f,
		Candidate: c,
		Leader:    l,
	}

	defer func() {
		if r := recover(); r != nil {
			if opErr, ok := r.(OpError); ok {
				err = opErr
			} else {
				panic(r)
			}
		}
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
			case <-r.shutdownCh:
				return ErrServerClosed

			case rpc := <-r.server.rpcCh:
				resetTimer := r.replyRPC(rpc)
				// on receiving AppendEntries from current leader or
				// granting vote to candidate reset timer
				if r.state == Follower && resetTimer {
					f.resetTimer()
				}

			case <-r.timer.C:
				r.timer.active = false
				ships[r.state].onTimeout()

			case ne := <-r.newEntryCh:
				if r.state == Leader {
					_ = l.storeEntry(ne)
				} else {
					ne.reply(NotLeaderError{f.leaderAddr(), false, nil})
				}

			case t := <-r.taskCh:
				f.executeTask(t)
				if r.state == Follower && f.electionAborted {
					f.resetTimer()
				}

			case t := <-r.snapTakenCh:
				r.onSnapshotTaken(t)

			// candidate --------------
			case v := <-c.voteCh:
				assert(r.state == Candidate, "M%d BUG: %v", r.id, r.state)
				c.onVoteResult(v)

			// leader --------------
			case u := <-l.fromReplsCh:
				assert(r.state == Leader, "M%d BUG: %v", r.id, r.state)
				l.checkReplUpdates(u)

			case <-l.transferTimer.C:
				l.transferTimer.active = false
				l.onTransferTimeout()

			case err := <-l.transferLdr.rpcCh:
				l.onTimeoutNow(err)
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
}

func (r *Raft) Shutdown() *sync.WaitGroup {
	r.shutdownMu.Lock()
	defer r.shutdownMu.Unlock()
	if !r.isClosed() {
		debug(r.id, ">> shutdown()")
		if r.trace.ShuttingDown != nil {
			r.trace.ShuttingDown(r.liveInfo())
		}
		close(r.shutdownCh)
	}
	return &r.wg
}

func (r *Raft) Closed() <-chan struct{} {
	return r.shutdownCh
}

// tells whether shutdown was called
func (r *Raft) isClosed() bool {
	select {
	case <-r.shutdownCh:
		return true
	default:
		return false
	}
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

func (r *Raft) ID() uint64 {
	return r.id
}

func (r *Raft) FSM() FSM {
	return r.fsm.FSM
}

func (r *Raft) addr() string {
	if self, ok := r.configs.Latest.Nodes[r.id]; ok {
		return self.Addr
	}
	return ""
}

func (r *Raft) leaderAddr() string {
	if r.leader == 0 {
		return ""
	}
	if ldr, ok := r.configs.Latest.Nodes[r.leader]; ok {
		return ldr.Addr
	}
	return ""
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
