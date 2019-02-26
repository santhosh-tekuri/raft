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
	HeartbeatTimeout time.Duration
}

func DefaultOptions() Options {
	return Options{
		HeartbeatTimeout: 1000 * time.Millisecond,
	}
}

type Raft struct {
	*server
	dialFn dialFn

	id      NodeID
	addr    string
	configs Configs
	wg      sync.WaitGroup

	fsmApplyCh chan NewEntry
	fsm        FSM

	storage *Storage
	term    uint64
	state   State
	leader  string

	votedFor  string
	hbTimeout time.Duration

	lastLogIndex uint64
	lastLogTerm  uint64
	commitIndex  uint64
	lastApplied  uint64

	connPools map[string]*connPool

	taskCh     chan Task
	newEntryCh chan NewEntry
	trace      Trace
	shutdownCh chan struct{}
}

func New(id NodeID, addr string, opt Options, fsm FSM, storage *Storage, trace Trace) (*Raft, error) {
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

	server := newServer(2 * opt.HeartbeatTimeout)
	return &Raft{
		id:           id,
		addr:         addr,
		storage:      storage,
		fsm:          fsm,
		term:         term,
		votedFor:     votedFor,
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastLogTerm,
		configs:      configs,
		state:        Follower,
		hbTimeout:    opt.HeartbeatTimeout,
		dialFn:       net.DialTimeout,
		server:       server,
		connPools:    make(map[string]*connPool),
		fsmApplyCh:   make(chan NewEntry, 128), // todo configurable capacity
		newEntryCh:   make(chan NewEntry, 100), // todo configurable capacity
		taskCh:       make(chan Task, 100),     // todo configurable capacity
		trace:        trace,
		shutdownCh:   make(chan struct{}),
	}, nil
}

// todo: note that we dont support multiple listeners
func (r *Raft) Serve(l net.Listener) error {
	r.wg.Add(2)
	go r.loop()
	go r.fsmLoop()
	return r.server.serve(l)
}

func (r *Raft) Shutdown() {
	debug(r.addr, ">> shutdown()")
	close(r.shutdownCh)
	r.wg.Wait()
	debug(r.addr, "<< shutdown()")
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

func (r *Raft) setVotedFor(v string) {
	if err := r.storage.vars.SetVote(r.term, v); err != nil {
		panic(fmt.Sprintf("save votedFor failed: %v", err))
	}
	r.votedFor = v
}

func (r *Raft) getConnPool(addr string) *connPool {
	pool, ok := r.connPools[addr]
	if !ok {
		pool = &connPool{
			addr:    addr,
			dialFn:  r.dialFn,
			timeout: 10 * time.Second, // todo
			max:     3,                //todo
		}
		r.connPools[addr] = pool
	}
	return pool
}

type NotLeaderError struct {
	Leader string
}

func (e NotLeaderError) Error() string {
	return "node is not the leader"
}

func afterRandomTimeout(min time.Duration) <-chan time.Time {
	return time.After(min + time.Duration(rand.Int63())%min)
}
