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

func ApplyEntry(data []byte) Task {
	return newEntry{
		task: &task{done: make(chan struct{})},
		entry: &entry{
			typ:  entryCommand,
			data: data,
		},
	}
}

// ------------------------------------------------------------------------

type bootstrap struct {
	*task
	addrs []string
}

func Bootstrap(addrs []string) Task {
	return bootstrap{
		task:  &task{done: make(chan struct{})},
		addrs: addrs,
	}
}

var ErrCantBootstrap = errors.New("raft: bootstrap only works on new clusters")

func (r *Raft) bootstrap(t bootstrap) {
	debug(r, "bootstrapping....")
	// todo: validate addrs
	addrsMap := make(map[string]struct{})
	for _, addr := range t.addrs {
		addrsMap[addr] = struct{}{}
	}
	if len(t.addrs) != len(addrsMap) {
		t.reply("bootstrap: duplicate address")
		return
	}
	if _, ok := addrsMap[r.addr]; !ok {
		t.reply(fmt.Errorf("bootstrap: myself %s must be part of cluster", r.addr))
		return
	}

	// todo: check whether bootstrap is allowed ?
	if r.term != 0 || r.lastLogIndex != 0 {
		t.reply(ErrCantBootstrap)
		return
	}

	// persist config change
	configEntry, err := r.storage.bootstrap(t.addrs)
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

func (r *Raft) executeTask(t Task) {
	switch t := t.(type) {
	case bootstrap:
		r.bootstrap(t)
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
