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
	Trace              Trace
	Resolver           Resolver
}

func DefaultOptions() Options {
	var mu sync.Mutex
	logger := func(prefix string) func(format string, v ...interface{}) {
		return func(format string, v ...interface{}) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Print(prefix)
			fmt.Printf(format, v...)
		}
	}
	return Options{
		HeartbeatTimeout:   1000 * time.Millisecond,
		LeaderLeaseTimeout: 1000 * time.Millisecond,
		Trace:              DefaultTrace(logger("[INFO] "), logger("[WARN] ")),
	}
}

type Raft struct {
	id     ID
	server *server
	trace  Trace

	fsm        FSM
	fsmApplyCh chan NewEntry

	storage      *Storage
	term         uint64
	votedFor     ID
	lastLogIndex uint64
	lastLogTerm  uint64
	configs      Configs

	state  State
	leader ID

	commitIndex uint64
	lastApplied uint64

	hbTimeout       time.Duration
	ldrLeaseTimeout time.Duration

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

func New(id ID, opt Options, fsm FSM, storage *Storage) (*Raft, error) {
	if err := storage.init(); err != nil {
		return nil, err
	}

	term, votedFor, err := storage.vars.GetVote()
	if err != nil {
		return nil, err
	}

	var lastLogIndex, lastLogTerm uint64
	last, err := storage.lastEntry()
	if err != nil {
		return nil, err
	}
	if last != nil {
		lastLogIndex, lastLogTerm = last.index, last.term
	}

	configs, err := storage.getConfigs()
	if err != nil {
		return nil, err
	}

	resolver := &resolver{
		delegate: opt.Resolver,
		addrs:    make(map[ID]string),
	}
	resolver.update(configs.Latest)

	server := newServer(2 * opt.HeartbeatTimeout)
	r := &Raft{
		id:              id,
		storage:         storage,
		fsm:             fsm,
		term:            term,
		votedFor:        ID(votedFor),
		lastLogIndex:    lastLogIndex,
		lastLogTerm:     lastLogTerm,
		configs:         configs,
		state:           Follower,
		hbTimeout:       opt.HeartbeatTimeout,
		ldrLeaseTimeout: opt.LeaderLeaseTimeout,
		resolver:        resolver,
		dialFn:          net.DialTimeout,
		server:          server,
		connPools:       make(map[ID]*connPool),
		fsmApplyCh:      make(chan NewEntry, 128), // todo configurable capacity
		newEntryCh:      make(chan NewEntry, 100), // todo configurable capacity
		taskCh:          make(chan Task, 100),     // todo configurable capacity
		trace:           opt.Trace,
		shutdownCh:      make(chan struct{}),
	}
	resolver.trace = &r.trace
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
		r.wg.Add(2)
	}
	r.shutdownMu.Unlock()
	if shutdownCalled {
		return ErrServerClosed
	}

	if r.trace.Starting != nil {
		r.trace.Starting(r.liveInfo())
	}
	go r.loop()
	go r.fsmLoop()
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

func (r *Raft) loop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.shutdownCh:
			debug(r, "loop shutdown")
			r.server.shutdown()
			debug(r, "server shutdown")
			close(r.fsmApplyCh)
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

func (r *Raft) setTerm(term uint64) {
	if err := r.storage.vars.SetVote(term, ""); err != nil {
		panic(fmt.Sprintf("stable.Set failed: %v", err))
	}
	r.term, r.votedFor = term, ""
}

func (r *Raft) setVotedFor(v ID) {
	if err := r.storage.vars.SetVote(r.term, string(v)); err != nil {
		panic(fmt.Sprintf("save votedFor failed: %v", err))
	}
	r.votedFor = v
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
