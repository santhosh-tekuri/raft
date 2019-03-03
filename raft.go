package raft

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

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

type Options struct {
	HeartbeatTimeout   time.Duration
	LeaderLeaseTimeout time.Duration
	SnapshotThreshold  uint64
	Trace              Trace
	Resolver           Resolver
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
	return Options{
		HeartbeatTimeout:   1000 * time.Millisecond,
		LeaderLeaseTimeout: 1000 * time.Millisecond,
		Trace:              DefaultTrace(logger("[INFO]"), logger("[WARN]")),
	}
}

type Raft struct {
	id     ID
	server *server

	fsm           FSM
	fsmTaskCh     chan Task
	snapTaskCh    chan takeSnapshot
	snapThreshold uint64

	// persistent state
	*storage

	// volatile state
	state       State
	leader      ID
	commitIndex uint64
	lastApplied uint64

	// options
	hbTimeout       time.Duration
	ldrLeaseTimeout time.Duration
	trace           Trace

	// dialing peers
	resolver  *resolver
	dialFn    dialFn // used for mocking in tests
	connPools map[ID]*connPool

	ldr        *leadership
	taskCh     chan Task
	newEntryCh chan NewEntry

	wg         sync.WaitGroup
	shutdownMu sync.Mutex
	shutdownCh chan struct{}
}

func New(id ID, opt Options, fsm FSM, storage Storage) (*Raft, error) {
	store := newStorage(storage)
	if err := store.init(); err != nil {
		return nil, err
	}

	r := &Raft{
		id:              id,
		server:          newServer(2 * opt.HeartbeatTimeout),
		fsm:             fsm,
		fsmTaskCh:       make(chan Task, 128), // todo configurable capacity
		snapTaskCh:      make(chan takeSnapshot),
		snapThreshold:   opt.SnapshotThreshold,
		storage:         store,
		state:           Follower,
		hbTimeout:       opt.HeartbeatTimeout,
		ldrLeaseTimeout: opt.LeaderLeaseTimeout,
		trace:           opt.Trace,
		dialFn:          net.DialTimeout,
		connPools:       make(map[ID]*connPool),
		taskCh:          make(chan Task, 100),     // todo configurable capacity
		newEntryCh:      make(chan NewEntry, 100), // todo configurable capacity
		shutdownCh:      make(chan struct{}),
	}

	r.resolver = &resolver{
		delegate: opt.Resolver,
		addrs:    make(map[ID]string),
		trace:    &r.trace,
	}
	r.resolver.update(store.configs.Latest)

	return r, nil
}

func (r *Raft) ID() ID {
	return r.id
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

func (r *Raft) FSM() FSM {
	return r.fsm
}

// tells whether shutdown was called
func (r *Raft) shutdownCalled() bool {
	select {
	case <-r.shutdownCh:
		return true
	default:
		return false
	}
}

// todo: note that we dont support multiple listeners

func (r *Raft) Serve(l net.Listener) error {
	r.shutdownMu.Lock()
	shutdownCalled := r.shutdownCalled()
	if !shutdownCalled {
		r.wg.Add(3)
	}
	r.shutdownMu.Unlock()
	if shutdownCalled {
		return ErrServerClosed
	}

	if r.trace.Starting != nil {
		r.trace.Starting(r.liveInfo())
	}

	go r.fsmLoop()
	// restore fsm from last snapshot, if present
	if r.snapIndex > 0 {
		req := fsmRestoreReq{task: newTask(), index: r.snapIndex}
		r.fsmTaskCh <- req
		<-req.Done()
		if req.Err() != nil {
			r.commitIndex, r.lastApplied = r.snapIndex, r.snapIndex
		}
	}

	go r.stateLoop()
	go r.snapLoop()

	return r.server.serve(l)
}

func (r *Raft) Shutdown() *sync.WaitGroup {
	r.shutdownMu.Lock()
	defer r.shutdownMu.Unlock()
	if !r.shutdownCalled() {
		debug(r.id, ">> shutdown()")
		if r.trace.ShuttingDown != nil {
			r.trace.ShuttingDown(r.liveInfo())
		}
		close(r.shutdownCh)
	}
	return &r.wg
}

func (r *Raft) stateLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.shutdownCh:
			debug(r, "stateLoop shutdown")
			r.server.shutdown()
			debug(r, "server shutdown")
			return
		default:
		}

		switch r.state {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

func (r *Raft) getConnPool(id ID) *connPool {
	pool, ok := r.connPools[id]
	if !ok {
		pool = &connPool{
			id:       id,
			resolver: r.resolver,
			dialFn:   r.dialFn,
			timeout:  10 * time.Second, // todo
			max:      3,                //todo
		}
		r.connPools[id] = pool
	}
	return pool
}

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

func afterRandomTimeout(min time.Duration) <-chan time.Time {
	return time.After(min + time.Duration(rand.Int63())%min)
}
