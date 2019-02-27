package raft

import (
	"errors"
	"fmt"
)

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
	term, votedFor, err := r.storage.vars.GetVote()
	if err != nil {
		t.reply(err)
	}

	// everything is ok. bootstrapping now...
	r.term, r.votedFor = term, votedFor
	r.lastLogIndex, r.lastLogTerm = e.index, e.term
	r.configs.Latest = configEntry
	t.reply(nil)
}

func (ldr *leadership) addNode(t addNode) {
	if _, ok := ldr.configs.Latest.Nodes[t.node.ID]; ok {
		t.reply(fmt.Errorf("raft.addNode: node %s already exists", t.node.ID))
	}
	if t.node.Type == Voter {
		t.reply(errors.New("raft.addNode: new node cannot be voter, add to staging"))
	}
	newConfig := ldr.configs.Latest.clone()
	newConfig.Nodes[t.node.ID] = t.node
	ldr.storeConfig(t.task, newConfig)
}

func (ldr *leadership) removeNode(t removeNode) {
	if _, ok := ldr.configs.Latest.Nodes[t.id]; !ok {
		t.reply(fmt.Errorf("raft.removeNode: node %s does not exist", t.id))
	}
	newConfig := ldr.configs.Latest.clone()
	delete(newConfig.Nodes, t.id)
	ldr.storeConfig(t.task, newConfig)
}

func (ldr *leadership) storeConfig(t *task, newConfig Config) {
	// validate
	if !ldr.configs.IsCommitted() {
		t.reply(errors.New("raft: configChange is in progress"))
		return
	}
	if ldr.commitIndex < ldr.startIndex {
		t.reply(errors.New("raft: noop entry is not yet committed"))
		return
	}
	if err := newConfig.validate(); err != nil {
		t.reply(err)
		return
	}

	// append to log
	ne := NewEntry{
		entry: newConfig.encode(),
		task:  t,
	}
	ldr.storeEntry(ne)
	newConfig.Index, newConfig.Term = ne.index, ne.term
	ldr.changeConfig(newConfig)

	// now majority might have changed. needs to be recalculated
	ldr.onMajorityCommit()
}
