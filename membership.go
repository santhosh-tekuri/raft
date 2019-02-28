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
		return
	}
	term, votedFor, err := r.storage.vars.GetVote()
	if err != nil {
		t.reply(err)
		return
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
		return
	}
	if t.node.Voter {
		t.reply(errors.New("raft.addNode: new node cannot be voter"))
		return
	}
	newConfig := ldr.configs.Latest.clone()
	newConfig.Nodes[t.node.ID] = t.node
	if err := ldr.storeConfig(t.task, newConfig); err != nil {
		t.reply(err)
		return
	}
	debug(ldr, "addNode", t.node)
	ldr.startReplication(t.node)
}

func (ldr *leadership) removeNode(t removeNode) {
	if _, ok := ldr.configs.Latest.Nodes[t.id]; !ok {
		t.reply(fmt.Errorf("raft.removeNode: node %s does not exist", t.id))
		return
	}
	newConfig := ldr.configs.Latest.clone()
	delete(newConfig.Nodes, t.id)
	if err := ldr.storeConfig(t.task, newConfig); err != nil {
		t.reply(err)
		return
	}

	// stop replication
	debug(ldr, "removeNode", t.id)
	repl := ldr.repls[t.id]
	delete(ldr.repls, t.id)
	close(repl.stopCh)

	// now majority might have changed. needs to be recalculated
	ldr.onMajorityCommit()
}

func (ldr *leadership) storeConfig(t *task, newConfig Config) error {
	// validate
	if !ldr.configs.IsCommitted() {
		return errors.New("raft: configChange is in progress")
	}
	if ldr.commitIndex < ldr.startIndex {
		return errors.New("raft: noop entry is not yet committed")
	}
	if err := newConfig.validate(); err != nil {
		return err
	}

	// append to log
	ne := NewEntry{
		entry: newConfig.encode(),
		task:  t,
	}
	ldr.storeEntry(ne)
	newConfig.Index, newConfig.Term = ne.index, ne.term
	ldr.changeConfig(newConfig)
	return nil
}
