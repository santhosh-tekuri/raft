package raft

import (
	"container/list"
	"net"
	"sync"
	"time"
)

type Raft struct {
	id     ID
	rtime  randTime
	timer  *safeTimer
	server *server

	fsm           FSM
	fsmTaskCh     chan Task
	snapTakenCh   chan snapTaken // non nil only when snapshot task is in progress
	snapThreshold uint64

	// persistent state
	*storage

	// volatile state
	state       State
	leader      ID
	commitIndex uint64
	lastApplied uint64

	// options
	hbTimeout        time.Duration
	ldrLeaseTimeout  time.Duration
	promoteThreshold time.Duration
	trace            Trace

	// dialing flrs
	resolver  *resolver
	dialFn    dialFn // used for mocking in tests
	connPools map[ID]*connPool

	ldr        *ldrShip
	taskCh     chan Task
	newEntryCh chan NewEntry

	wg         sync.WaitGroup
	shutdownMu sync.Mutex
	shutdownCh chan struct{}
}

func New(id ID, opt Options, fsm FSM, storage Storage) (*Raft, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	store := newStorage(storage)
	if err := store.init(); err != nil {
		return nil, err
	}

	r := &Raft{
		id:               id,
		rtime:            newRandTime(),
		timer:            newSafeTimer(),
		server:           newServer(),
		fsm:              fsm,
		fsmTaskCh:        make(chan Task, 128), // todo configurable capacity
		snapThreshold:    opt.SnapshotThreshold,
		storage:          store,
		state:            Follower,
		hbTimeout:        opt.HeartbeatTimeout,
		ldrLeaseTimeout:  opt.LeaderLeaseTimeout,
		promoteThreshold: opt.PromoteThreshold,
		trace:            opt.Trace,
		dialFn:           net.DialTimeout,
		connPools:        make(map[ID]*connPool),
		taskCh:           make(chan Task),
		newEntryCh:       make(chan NewEntry),
		shutdownCh:       make(chan struct{}),
	}

	r.resolver = &resolver{
		delegate: opt.Resolver,
		addrs:    make(map[ID]string),
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

	go r.fsmLoop()
	// restore fsm from last snapshot, if present
	if r.snapIndex > 0 {
		if err := r.restoreFSMFromSnapshot(); err != nil {
			close(r.fsmTaskCh) // to stop fsmLoop
			return err
		}
	}

	r.wg.Add(1)
	go r.server.serve(l)

	r.stateLoop()
	return ErrServerClosed
}

func (r *Raft) stateLoop() {
	var (
		f = &flrShip{Raft: r}
		c = &candShip{Raft: r}
		l = &ldrShip{
			Raft:       r,
			newEntries: list.New(),
			flrs:       make(map[ID]*flr),
		}
	)
	r.ldr = l
	ships := map[State]stateShip{
		Follower:  f,
		Candidate: c,
		Leader:    l,
	}

	for {
		rstate := r.state
		ships[rstate].init()
		for r.state == rstate {
			select {
			case <-r.shutdownCh:
				ships[rstate].release()
				r.release()
				return

			case rpc := <-r.server.rpcCh:
				resetTimer := r.replyRPC(rpc)
				// on receiving AppendEntries from current leader or
				// granting vote to candidate reset timer
				if r.state == Follower && resetTimer {
					f.resetTimer()
				}

			case <-r.timer.C:
				r.timer.receive = false
				ships[r.state].onTimeout()

			case ne := <-r.newEntryCh:
				if r.state == Leader {
					l.storeEntry(ne)
				} else {
					ne.reply(NotLeaderError{f.leaderAddr(), false})
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
				assert(r.state == Candidate, "%s BUG: %v", r.id, r.state)
				c.onVoteResult(v)

			// leader --------------
			case u := <-l.fromReplsCh:
				assert(r.state == Leader, "%s BUG: %v", r.id, r.state)
				l.checkReplUpdates(u)
			}
		}
		if r.trace.StateChanged != nil {
			r.trace.StateChanged(r.liveInfo())
		}
		r.timer.stop()
		ships[rstate].release()
	}
}

// closing -----------------------------------------

func (r *Raft) release() {
	r.server.shutdown()
	debug(r, "server shutdown")
	close(r.fsmTaskCh)

	// wait for snapshot to complete
	if r.snapTakenCh != nil {
		r.onSnapshotTaken(<-r.snapTakenCh)
	}
	r.wg.Done()
	debug(r, "stateLoop shutdown")
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

func (r *Raft) ID() ID {
	return r.id
}

func (r *Raft) FSM() FSM {
	return r.fsm
}

func (r *Raft) addr() string {
	if self, ok := r.configs.Latest.Nodes[r.id]; ok {
		return self.Addr
	}
	return ""
}

func (r *Raft) leaderAddr() string {
	if r.leader == "" {
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
