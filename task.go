package raft

import (
	"errors"
	"fmt"
)

type Task interface {
	execute(r *Raft)
	Done() <-chan struct{}
	Err() error
	Result() interface{}
	reply(interface{})
}

type task struct {
	fn     func(t Task, r *Raft)
	result interface{}
	done   chan struct{}
}

func (t *task) execute(r *Raft) {
	t.fn(t, r)
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

func ApplyEntry(data []byte) Task {
	return &task{
		fn: func(t Task, r *Raft) {
			applyEntry(t, r, data)
		},
		done: make(chan struct{}),
	}
}

func applyEntry(t Task, r *Raft, data []byte) {
	if r.state != leader {
		t.reply(NotLeaderError{r.leaderID})
		return
	}
	r.leadership.applyEntry(newEntry{
		entry: &entry{
			typ:  entryCommand,
			data: data,
		},
		task: t,
	})
}

// ------------------------------------------------------------------------

var ErrCantBootstrap = errors.New("raft: bootstrap only works on new clusters")

func Bootstrap(addrs []string) Task {
	return &task{
		fn: func(t Task, r *Raft) {
			bootstrap(t, r, addrs)
		},
		done: make(chan struct{}),
	}
}

func bootstrap(t Task, r *Raft, addrs []string) {
	debug(r, "bootstrapping....")
	// todo: validate addrs
	addrsMap := make(map[string]struct{})
	for _, addr := range addrs {
		addrsMap[addr] = struct{}{}
	}
	if len(addrs) != len(addrsMap) {
		t.reply("Raft.bootstrap: duplicate address")
		return
	}
	if _, ok := addrsMap[r.addr]; !ok {
		t.reply(fmt.Errorf("Raft.bootstrap: myself %s must be part of cluster", r.addr))
		return
	}

	// todo: check whether bootstrap is allowed ?
	if r.term != 0 || r.lastLogIndex != 0 {
		t.reply(ErrCantBootstrap)
		return
	}

	// persist config change
	configEntry, err := r.storage.bootstrap(addrs)
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
