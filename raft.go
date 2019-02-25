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

type Raft struct {
	*server
	dialFn dialFn

	id      NodeID
	addr    string
	configs Configs
	wg      sync.WaitGroup

	fsmApplyCh chan newEntry
	fsm        FSM

	storage *storage
	term    uint64
	state   State
	leader  string

	votedFor         string
	heartbeatTimeout time.Duration

	lastLogIndex uint64
	lastLogTerm  uint64
	commitIndex  uint64
	lastApplied  uint64

	connPools map[string]*connPool

	taskCh     chan Task
	shutdownCh chan struct{}
}

func New(id NodeID, addr string, fsm FSM, stable Stable, log Log) (*Raft, error) {
	storage := &storage{Stable: stable, log: log}
	if err := storage.init(); err != nil {
		return nil, err
	}

	term, votedFor, err := storage.GetVars()
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

	configs := Configs{}
	committed, latest, err := storage.GetConfig()
	if err != nil {
		return nil, err
	}
	if committed != 0 {
		e := &entry{}
		storage.getEntry(committed, e)
		if err := configs.Committed.decode(e); err != nil {
			return nil, err
		}
	}
	if latest != 0 {
		e := &entry{}
		storage.getEntry(latest, e)
		if err := configs.Latest.decode(e); err != nil {
			return nil, err
		}
	}

	heartbeatTimeout := 50 * time.Millisecond // todo,
	server := &server{
		listenFn:              net.Listen,
		shutdownCheckDuration: heartbeatTimeout,
	}
	return &Raft{
		id:               id,
		addr:             addr,
		storage:          storage,
		fsm:              fsm,
		term:             term,
		votedFor:         votedFor,
		lastLogIndex:     lastLogIndex,
		lastLogTerm:      lastLogTerm,
		configs:          configs,
		state:            Follower,
		heartbeatTimeout: heartbeatTimeout,
		dialFn:           net.DialTimeout,
		server:           server,
		connPools:        make(map[string]*connPool),
		fsmApplyCh:       make(chan newEntry, 128), // todo configurable capacity
		taskCh:           make(chan Task, 100),     // todo configurable capacity
		shutdownCh:       make(chan struct{}),
	}, nil
}

func (r *Raft) ListenAndServe() error {
	if err := r.Listen(); err != nil {
		return err
	}
	return r.Serve()
}

func (r *Raft) Listen() error {
	return r.server.listen(r.addr)
}

func (r *Raft) Serve() error {
	r.wg.Add(2)
	go r.loop()
	go r.fsmLoop()
	return r.server.serve()
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
	if err := r.storage.SetVars(term, ""); err != nil {
		panic(fmt.Sprintf("stable.Set failed: %v", err))
	}
	r.term, r.votedFor = term, ""
}

func (r *Raft) setVotedFor(v string) {
	if err := r.storage.SetVars(r.term, v); err != nil {
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

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

// backoff is used to compute an exponential backoff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor.
func backoff(round uint64) time.Duration {
	base, limit := failureWait, uint64(maxFailureScale)
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	return base
}

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
