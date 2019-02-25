package raft

import (
	"errors"
	"fmt"
)

func (r *Raft) Tasks() chan<- Task {
	return r.taskCh
}

type Task interface {
	Done() <-chan struct{}
	Err() error
	Result() interface{}
	reply(interface{})
}

// ---------------------------------------

type task struct {
	result interface{}
	done   chan struct{}
}

func (t *task) Done() <-chan struct{} {
	return t.done
}

func (t *task) Err() error {
	if err, ok := t.result.(error); ok {
		return err
	}
	return nil
}

func (t *task) Result() interface{} {
	if _, ok := t.result.(error); ok {
		return nil
	}
	return t.result
}

func (t *task) reply(result interface{}) {
	if t != nil {
		t.result = result
		if t.done != nil {
			close(t.done)
		}
	}
}

// ------------------------------------------------------------------------

type newEntry struct {
	*task
	*entry
}

func ApplyCommand(data []byte) Task {
	return newEntry{
		task: &task{done: make(chan struct{})},
		entry: &entry{
			typ:  entryCommand,
			data: data,
		},
	}
}

// Barrier is used to issue a command that blocks until all preceding
// commands have been applied to the FSM. It can be used to ensure the
// FSM reflects all queued commands.
func Barrier() Task {
	return newEntry{
		task: &task{done: make(chan struct{})},
		entry: &entry{
			typ: entryBarrier,
		},
	}
}

// ------------------------------------------------------------------------

type bootstrap struct {
	*task
	nodes map[NodeID]Node
}

func Bootstrap(nodes map[NodeID]Node) Task {
	return bootstrap{
		task:  &task{done: make(chan struct{})},
		nodes: nodes,
	}
}

var ErrCantBootstrap = errors.New("raft: bootstrap only works on new clusters")

func (r *Raft) bootstrap(t bootstrap) {
	debug(r, "bootstrapping....")

	// validations
	self, ok := t.nodes[r.id]
	if !ok {
		t.reply(fmt.Errorf("bootstrap: myself %s must be part of cluster", r.id))
		return
	}
	if self.Addr != r.addr { // todo: allow changing advertise address
		t.reply(fmt.Errorf("bootstrap: my address does not match"))
		return
	}

	// todo: check whether bootstrap is allowed ?
	if r.term != 0 || r.lastLogIndex != 0 {
		t.reply(ErrCantBootstrap)
		return
	}

	// persist config change
	configEntry, err := r.storage.bootstrap(t.nodes)
	if err != nil {
		t.reply(err)
		return
	}
	e, err := r.storage.lastEntry()
	if err != nil {
		t.reply(err)
	}
	term, votedFor, err := r.storage.GetVars()
	if err != nil {
		t.reply(err)
	}

	// everything is ok. bootstrapping now...
	r.term, r.votedFor = term, votedFor
	r.lastLogIndex, r.lastLogTerm = e.index, e.term
	r.configs.Latest = configEntry
	t.reply(nil)
}

// ------------------------------------------------------------------------

type Info interface {
	ID() NodeID
	Addr() string
	Term() uint64
	State() State
	LeaderID() NodeID
	LeaderAddr() string
	LastLogIndex() uint64
	LastLogTerm() uint64
	Committed() uint64
	LastApplied() uint64
	Configs() Configs
	MatchIndexes() map[NodeID]uint64
}

type liveInfo struct {
	r   *Raft
	ldr *leadership
}

func (info liveInfo) ID() NodeID           { return info.r.id }
func (info liveInfo) Addr() string         { return info.r.addr }
func (info liveInfo) Term() uint64         { return info.r.term }
func (info liveInfo) State() State         { return info.r.state }
func (info liveInfo) LeaderAddr() string   { return info.r.leader }
func (info liveInfo) LastLogIndex() uint64 { return info.r.lastLogIndex }
func (info liveInfo) LastLogTerm() uint64  { return info.r.lastLogTerm }
func (info liveInfo) Committed() uint64    { return info.r.commitIndex }
func (info liveInfo) LastApplied() uint64  { return info.r.lastApplied }
func (info liveInfo) Configs() Configs     { return info.r.configs.clone() }

func (info liveInfo) LeaderID() NodeID {
	for _, node := range info.r.configs.Latest.Nodes {
		if node.Addr == info.r.leader {
			return node.ID
		}
	}
	return NodeID("")
}

func (info liveInfo) MatchIndexes() map[NodeID]uint64 {
	if info.ldr == nil {
		return nil
	}
	m := make(map[NodeID]uint64)
	for id, repl := range info.ldr.repls {
		m[id] = repl.status.matchIndex
	}
	return m
}

type cachedInfo struct {
	id           NodeID
	addr         string
	term         uint64
	state        State
	leaderID     NodeID
	leaderAddr   string
	lastLogIndex uint64
	lastLogTerm  uint64
	committed    uint64
	lastApplied  uint64
	configs      Configs
	matchIndexes map[NodeID]uint64
}

func (info cachedInfo) ID() NodeID                      { return info.id }
func (info cachedInfo) Addr() string                    { return info.addr }
func (info cachedInfo) Term() uint64                    { return info.term }
func (info cachedInfo) State() State                    { return info.state }
func (info cachedInfo) LeaderID() NodeID                { return info.leaderID }
func (info cachedInfo) LeaderAddr() string              { return info.leaderAddr }
func (info cachedInfo) LastLogIndex() uint64            { return info.lastLogIndex }
func (info cachedInfo) LastLogTerm() uint64             { return info.lastLogTerm }
func (info cachedInfo) Committed() uint64               { return info.committed }
func (info cachedInfo) LastApplied() uint64             { return info.lastApplied }
func (info cachedInfo) Configs() Configs                { return info.configs }
func (info cachedInfo) MatchIndexes() map[NodeID]uint64 { return info.matchIndexes }

type inspect struct {
	*task
	fn func(api Info)
}

func Inspect(fn func(r Info)) Task {
	return inspect{
		task: &task{done: make(chan struct{})},
		fn:   fn,
	}
}

func (r *Raft) Info() Info {
	var info Info
	task := Inspect(func(r Info) {
		info = cachedInfo{
			id:           r.ID(),
			addr:         r.Addr(),
			term:         r.Term(),
			state:        r.State(),
			leaderID:     r.LeaderID(),
			leaderAddr:   r.LeaderAddr(),
			lastLogIndex: r.LastLogIndex(),
			lastLogTerm:  r.LastLogTerm(),
			committed:    r.Committed(),
			lastApplied:  r.LastApplied(),
			configs:      r.Configs(),
			matchIndexes: r.MatchIndexes(),
		}
	})
	r.taskCh <- task
	<-task.Done()
	return info
}

// ------------------------------------------------------------------------

type addNode struct {
	*task
	node Node
}

func AddNode(node Node) Task {
	return addNode{
		task: &task{done: make(chan struct{})},
		node: node,
	}
}

// ------------------------------------------------------------------------

func (r *Raft) executeTask(t Task) {
	switch t := t.(type) {
	case bootstrap:
		r.bootstrap(t)
	case inspect:
		t.fn(liveInfo{r: r})
		t.reply(nil)
	default:
		t.reply(NotLeaderError{r.leader})
	}
}

func (ldr *leadership) executeTask(t Task) {
	switch t := t.(type) {
	case newEntry:
		ldr.applyEntry(t)
	case addNode:
		ldr.addNode(t)
	case inspect:
		t.fn(liveInfo{r: ldr.Raft, ldr: ldr})
		t.reply(nil)
	default:
		ldr.Raft.executeTask(t)
	}
}
