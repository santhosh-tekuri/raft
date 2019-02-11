package raft

import (
	"fmt"
	"math/rand"
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
	transport transport
	addr      string
	members   []*member
	wg        sync.WaitGroup

	fsmApplyCh chan newEntry
	fsm        FSM

	storage  *storage
	term     uint64
	server   *server
	state    state
	leaderID string

	votedFor         string
	heartbeatTimeout time.Duration
	electionTimer    *time.Timer
	lastContact      time.Time // last time we had contact from the leader node

	lastLogIndex uint64
	lastLogTerm  uint64
	commitIndex  uint64
	lastApplied  uint64

	applyCh    chan newEntry
	inspectCh  chan func(*Raft)
	shutdownCh chan struct{}
}

func New(addrs []string, fsm FSM, stable Stable, log Log) *Raft {
	heartbeatTimeout := 50 * time.Millisecond // todo

	members := make([]*member, len(addrs))
	for i, addr := range addrs {
		members[i] = &member{
			transport:        tcpTransport{},
			addr:             addr,
			timeout:          10 * time.Second, // todo
			heartbeatTimeout: heartbeatTimeout,
			leaderUpdateCh:   make(chan leaderUpdate, 1),
		}
	}

	return &Raft{
		transport:        tcpTransport{},
		addr:             addrs[0],
		fsmApplyCh:       make(chan newEntry, 128), // todo configurable capacity
		fsm:              fsm,
		storage:          &storage{Stable: stable, log: log},
		server:           &server{transport: tcpTransport{}},
		members:          members,
		state:            follower,
		heartbeatTimeout: heartbeatTimeout,
		applyCh:          make(chan newEntry, 100), // todo configurable capacity
		inspectCh:        make(chan func(*Raft)),
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
	debug(r.addr, "-> shutdown()")
	close(r.shutdownCh)
	r.wg.Wait()
	debug(r.addr, "shutdown() ->")
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

func (r *Raft) quorumSize() int {
	return len(r.members)/2 + 1
}

func (r *Raft) setTerm(term uint64) {
	if r.term == term {
		return
	}
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
	respCh chan<- interface{}
}

func (ne newEntry) sendResponse(resp interface{}) {
	if ne.respCh != nil {
		select {
		case ne.respCh <- resp:
		default:
			go func() {
				ne.respCh <- resp
			}()
		}
	}
}

type NotLeaderError struct {
	Leader string
}

func (e NotLeaderError) Error() string {
	return "node is not the leader"
}

func (r *Raft) Apply(cmd []byte, respCh chan<- interface{}) {
	r.applyCh <- newEntry{
		entry: &entry{
			typ:  entryCommand,
			data: cmd,
		},
		respCh: respCh,
	}
}

// inspect blocks until f got executed.
// It is safe to invoke this from multiple goroutines.
// used for testing purposes only
func (r *Raft) inspect(f func(*Raft)) {
	var wg sync.WaitGroup
	wg.Add(1)
	r.inspectCh <- func(r *Raft) {
		f(r)
		wg.Done()
	}
	wg.Wait()
}

func randomTimeout(min time.Duration) time.Duration {
	return min + time.Duration(rand.Int63())%min
}

func randomTimer(min time.Duration) *time.Timer {
	return time.NewTimer(randomTimeout(min))
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
