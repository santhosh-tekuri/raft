package raft

import (
	"container/list"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type Raft struct {
	rtime  randTime
	id     ID
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
		server:           newServer(time.Second),
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
			leaseTimer: time.NewTimer(time.Hour),
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

			// follower --------------
			case <-f.timeoutCh:
				f.onTimeout()

			// candidate --------------
			case vote := <-c.voteCh:
				c.onVoteResult(vote)

			case <-c.timeoutCh:
				c.startElection()

			// leader --------------
			case update := <-l.fromReplsCh:
				l.checkReplUpdates(update)

			case <-l.leaseTimer.C:
				l.checkLeaderLease()
			}
		}
		if r.trace.StateChanged != nil {
			r.trace.StateChanged(r.liveInfo())
		}
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
}

// options ----------------------------------

type Options struct {
	HeartbeatTimeout   time.Duration
	LeaderLeaseTimeout time.Duration
	PromoteThreshold   time.Duration
	SnapshotThreshold  uint64
	Trace              Trace
	Resolver           Resolver
}

func (o Options) validate() error {
	if o.HeartbeatTimeout == 0 {
		return errors.New("raft.options: HeartbeatTimeout is zero")
	}
	if o.LeaderLeaseTimeout == 0 {
		return errors.New("raft.options: LeaderLeaseTimeout is zero")
	}
	if o.PromoteThreshold == 0 {
		return errors.New("raft.options: PromoteThreshold is zero")
	}
	return nil
}

func DefaultOptions() Options {
	var mu sync.Mutex
	logger := func(prefix string) func(v ...interface{}) {
		return func(v ...interface{}) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Println(append(append([]interface{}(nil), prefix), v...))
		}
	}
	hbTimeout := 1000 * time.Millisecond
	return Options{
		HeartbeatTimeout:   hbTimeout,
		LeaderLeaseTimeout: hbTimeout,
		PromoteThreshold:   hbTimeout,
		Trace:              DefaultTrace(logger("[INFO]"), logger("[WARN]")),
	}
}

// errors ----------------------------------

var (
	// ErrServerClosed is returned by the Raft's Serve and ListenAndServe
	// methods after a call to Shutdown
	ErrServerClosed = errors.New("raft: server closed")

	// ErrAlreadyBootstrapped is returned when bootstrap task received
	// by already bootstrapped server
	ErrAlreadyBootstrapped    = errors.New("raft.bootstrap: already bootstrapped")
	ErrConfigChangeInProgress = errors.New("raft.configChange: another in progress")
	ErrNotCommitReady         = errors.New("raft.configChange: not ready to commit")
	ErrConfigChanged          = errors.New("raft.configChange: config changed meanwhile")
	ErrSnapshotThreshold      = errors.New("raft.takeSnapshot: not enough outstanding logs to snapshot")
	ErrSnapshotInProgress     = errors.New("raft.takeSnapshot: another snapshot in progress")
	ErrNoUpdates              = errors.New("raft.takeSnapshot: no updates to FSM")
)

// NotLeaderError is returned by non-leader node if it cannot
// complete a request or node lost its leadership before
// completing the request.
type NotLeaderError struct {
	// Leader is address of leader.
	//
	// It is empty string, if this node does not know current leader.
	Leader string

	// Lost is true, if the node lost its leadership before
	// completing the request.
	Lost bool
}

func (e NotLeaderError) Error() string {
	var contact string
	if e.Leader != "" {
		contact = ", contact " + e.Leader
	}
	if e.Lost {
		return "raft: Lost leadership" + contact
	}
	return "raft: this node is not the leader" + contact
}

// OpError is the error type usually returned when an error
// is detected at storage/fsm layer. This error hinders
// in raft cluster and thus needs user attention.
type OpError struct {
	// Op is the operation which caused the error, such as
	// "Log.Get" or "Snapshots.Open".
	Op string

	// Err is the error that occurred during the operation.
	Err error
}

func (e OpError) Error() string {
	return fmt.Sprintf("raft: %s: %v", e.Op, e.Err)
}

func opError(err error, format string, v ...interface{}) OpError {
	return OpError{
		Op:  fmt.Sprintf(format, v...),
		Err: err,
	}
}
