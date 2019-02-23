package raft

import (
	"errors"
	"fmt"
)

type Task interface {
	Done() <-chan struct{}
	Err() error
	Result() interface{}
	reply(interface{})
}

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
	r.configs.latest = configEntry
	t.reply(nil)
}

// ------------------------------------------------------------------------

type inspectRaft struct {
	*task
	fn func(*Raft)
}

func inspect(fn func(*Raft)) Task {
	return inspectRaft{
		task: &task{done: make(chan struct{})},
		fn:   fn,
	}
}

// ------------------------------------------------------------------------

type info struct {
	*task
}

type Info struct {
	ID               NodeID
	Term             uint64
	State            State
	Leader           string
	LastLogIndex     uint64
	LastLogTerm      uint64
	CommitIndex      uint64
	LastAppliedIndex uint64
}

func GetInfo() Task {
	return info{task: &task{done: make(chan struct{})}}
}

func (r *Raft) Info() Info {
	t := GetInfo()
	r.TasksCh <- t
	<-t.Done()
	return t.Result().(Info)
}

// ------------------------------------------------------------------------

func (r *Raft) executeTask(t Task) {
	switch t := t.(type) {
	case bootstrap:
		r.bootstrap(t)
	case info:
		t.reply(Info{
			ID:               r.id,
			Term:             r.term,
			State:            r.state,
			Leader:           r.leader,
			LastLogIndex:     r.lastLogIndex,
			LastLogTerm:      r.lastLogTerm,
			CommitIndex:      r.commitIndex,
			LastAppliedIndex: r.lastApplied,
		})
	case inspectRaft:
		t.fn(r)
		t.reply(nil)
	default:
		t.reply(NotLeaderError{r.leader})
	}
}

func (ldr *leadership) executeTask(t Task) {
	switch t := t.(type) {
	case newEntry:
		ldr.applyEntry(t)
	default:
		ldr.Raft.executeTask(t)
	}
}
