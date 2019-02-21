package raft

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

type state int

const (
	follower  state = 'F'
	candidate state = 'C'
	leader    state = 'L'
)

func (s state) String() string {
	return string(s)
}

type Raft struct {
	*server
	dialFn dialFn

	addr   string
	config config
	wg     sync.WaitGroup

	fsmApplyCh chan newEntry
	fsm        FSM

	storage  *storage
	term     uint64
	state    state
	leaderID string

	votedFor         string
	heartbeatTimeout time.Duration

	lastLogIndex uint64
	lastLogTerm  uint64
	commitIndex  uint64
	lastApplied  uint64

	ldrState   *leaderState
	TasksCh    chan Task
	shutdownCh chan struct{}
}

func New(addrs []string, fsm FSM, stable Stable, log Log) *Raft {
	heartbeatTimeout := 50 * time.Millisecond // todo
	storage := &storage{Stable: stable, log: log}

	members := make(map[string]*member)
	for _, addr := range addrs {
		members[addr] = &member{
			dialFn:  net.DialTimeout,
			addr:    addr,
			timeout: 10 * time.Second, // todo
		}
	}

	return &Raft{
		dialFn:           net.DialTimeout,
		addr:             addrs[0],
		fsmApplyCh:       make(chan newEntry, 128), // todo configurable capacity
		fsm:              fsm,
		storage:          storage,
		server:           &server{listenFn: net.Listen},
		config:           &stableConfig{members},
		state:            follower,
		heartbeatTimeout: heartbeatTimeout,
		TasksCh:          make(chan Task, 100), // todo configurable capacity
		shutdownCh:       make(chan struct{}),
	}
}

func (r *Raft) ListenAndServe() error {
	if err := r.Listen(); err != nil {
		return err
	}
	return r.Serve()
}

func (r *Raft) Listen() error {
	var err error

	if err = r.storage.init(); err != nil {
		return err
	}

	if r.term, r.votedFor, err = r.storage.GetVars(); err != nil {
		return err
	}

	last, err := r.storage.lastEntry()
	if err != nil {
		return err
	}
	if last != nil {
		r.lastLogIndex, r.lastLogTerm = last.index, last.term
	}

	if err = r.server.listen(r.addr); err != nil {
		return err
	}
	return nil
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
		case follower:
			r.runFollower()
		case candidate:
			r.runCandidate()
		case leader:
			r.runLeader()
		}
	}
}

func (r *Raft) String() string {
	return fmt.Sprintf("%s %d %s |", r.addr, r.term, r.state)
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

type newEntry struct {
	*entry
	task Task
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
