package raft

import (
	"errors"
	"fmt"
)

var ErrAlreadyBootstrapped = errors.New("raft.Bootstrap: already bootstrapped")
var ErrConfigChangeInProgress = errors.New("raft: configChange is in progress")
var ErrNotCommitReady = errors.New("raft: not ready to commit")

func (r *Raft) bootstrap(t bootstrap) {
	// validate
	if !r.configs.IsBootstrap() {
		t.reply(ErrAlreadyBootstrapped)
		return
	}
	err := func() error {
		addrs := make(map[string]bool)
		for id, node := range t.nodes {
			if err := node.validate(); err != nil {
				return err
			}
			if id != node.ID {
				return fmt.Errorf("id mismatch for %s", node.ID)
			}
			if addrs[node.Addr] {
				return fmt.Errorf("duplicate address %s", node.Addr)
			}
			addrs[node.Addr] = true
			if node.Voter {
				node.Promote = false
				t.nodes[id] = node
			}
		}
		self, ok := t.nodes[r.id]
		if !ok {
			return fmt.Errorf("self %s does not exist", r.id)
		}
		if !self.Voter {
			return fmt.Errorf("self %s must be voter", r.id)
		}
		return nil
	}()
	if err != nil {
		t.reply(fmt.Errorf("raft.Bootstrap: %v", err))
		return
	}

	config, err := r.storage.bootstrap(t.nodes)
	if err != nil {
		t.reply(err)
		return
	}
	term, votedFor, err := r.storage.vars.GetVote()
	if err != nil {
		t.reply(err)
		return
	}

	debug(r, "bootstrapping....")
	r.term, r.votedFor = term, ID(votedFor)
	r.lastLogIndex, r.lastLogTerm = config.Index, config.Term
	r.configs.Latest = config
	if r.trace.ConfigChanged != nil {
		r.trace.ConfigChanged(r.liveInfo())
	}
	t.reply(nil)
}

func (r *Raft) canChangeConfig() error {
	if !r.configs.IsCommitted() {
		return ErrConfigChangeInProgress
	}
	if r.ldr.commitIndex < r.ldr.startIndex {
		return ErrNotCommitReady
	}
	return nil
}

func (ldr *leadership) addNonvoter(t addNonvoter) {
	// validate
	if err := ldr.canChangeConfig(); err != nil {
		t.reply(err)
		return
	}
	err := func() error {
		if err := t.node.validate(); err != nil {
			return err
		}
		if t.node.Voter {
			return errors.New("must be nonvoter")
		}
		if _, ok := ldr.configs.Latest.Nodes[t.node.ID]; ok {
			return fmt.Errorf("node %s already exists", t.node.ID)
		}
		if n, ok := ldr.configs.Latest.nodeForAddr(t.node.Addr); ok {
			return fmt.Errorf("address %s, already used by %s", n.Addr, n.ID)
		}
		return nil
	}()
	if err != nil {
		t.reply(fmt.Errorf("raft.AddNonvoter: %v", err))
		return
	}

	config := ldr.configs.Latest.clone()
	config.Nodes[t.node.ID] = t.node
	ldr.storeConfig(t.task, config)
	debug(ldr, "addNonvoter", t.node)
	ldr.startReplication(t.node)
}

func (ldr *leadership) removeNode(t removeNode) {
	// validate
	if err := ldr.canChangeConfig(); err != nil {
		t.reply(err)
		return
	}
	err := func() error {
		if t.id == "" {
			return errors.New("empty node id")
		}
		n, ok := ldr.configs.Latest.Nodes[t.id]
		if !ok {
			return fmt.Errorf("node %s does not exist", t.id)
		}
		if n.Voter && ldr.configs.Latest.numVoters() == 1 {
			return errors.New("last voter cannot be removed")
		}
		return nil
	}()
	if err != nil {
		t.reply(fmt.Errorf("raft.RemoveNode: %v", err))
		return
	}

	config := ldr.configs.Latest.clone()
	delete(config.Nodes, t.id)
	ldr.storeConfig(t.task, config)

	// stop replication
	debug(ldr, "removeNode", t.id)
	repl := ldr.repls[t.id]
	close(repl.stopCh)
	delete(ldr.repls, t.id)

	// now majority might have changed. needs to be recalculated
	ldr.onMajorityCommit()
}

func (ldr *leadership) storeConfig(t *task, newConfig Config) {
	// append to log
	ne := NewEntry{
		entry: newConfig.encode(),
		task:  t,
	}
	ldr.storeEntry(ne)
	newConfig.Index, newConfig.Term = ne.index, ne.term
	ldr.changeConfig(newConfig)
}
