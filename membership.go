package raft

import (
	"errors"
	"fmt"
)

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

	debug(r, "bootstrapping....")
	config, err := r.storage.bootstrap(t.nodes)
	if err != nil {
		t.reply(err)
		return
	}
	r.changeConfig(config)
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

func (l *ldrShip) addNonvoter(t addNonvoter) {
	// validate
	if err := l.canChangeConfig(); err != nil {
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
		if _, ok := l.configs.Latest.Nodes[t.node.ID]; ok {
			return fmt.Errorf("node %s already exists", t.node.ID)
		}
		if n, ok := l.configs.Latest.nodeForAddr(t.node.Addr); ok {
			return fmt.Errorf("address %s, already used by %s", n.Addr, n.ID)
		}
		return nil
	}()
	if err != nil {
		t.reply(fmt.Errorf("raft.AddNonvoter: %v", err))
		return
	}

	config := l.configs.Latest.clone()
	config.Nodes[t.node.ID] = t.node
	l.storeConfig(t.task, config)
	debug(l, "addNonvoter", t.node)
	l.startReplication(t.node)
}

func (l *ldrShip) removeNode(t removeNode) {
	// validate
	if err := l.canChangeConfig(); err != nil {
		t.reply(err)
		return
	}
	err := func() error {
		if t.id == "" {
			return errors.New("empty node id")
		}
		n, ok := l.configs.Latest.Nodes[t.id]
		if !ok {
			return fmt.Errorf("node %s does not exist", t.id)
		}
		if n.Voter && l.configs.Latest.numVoters() == 1 {
			return errors.New("last voter cannot be removed")
		}
		return nil
	}()
	if err != nil {
		t.reply(fmt.Errorf("raft.RemoveNode: %v", err))
		return
	}

	config := l.configs.Latest.clone()
	delete(config.Nodes, t.id)
	l.storeConfig(t.task, config)

	// stop replication
	debug(l, "removeNode", t.id)
	repl := l.repls[t.id]
	close(repl.stopCh)
	delete(l.repls, t.id)

	// now majority might have changed. needs to be recalculated
	l.onMajorityCommit()
}

func (l *ldrShip) changeAddrs(t changeAddrs) {
	// validate
	if err := l.canChangeConfig(); err != nil {
		t.reply(err)
		return
	}
	err := func() error {
		addrs := make(map[string]bool)
		for id, addr := range t.addrs {
			if id == "" {
				return errors.New("empty node id")
			}
			if _, ok := l.configs.Latest.Nodes[id]; !ok {
				return fmt.Errorf("node %s does not exist", id)
			}
			if err := validateAddr(addr); err != nil {
				return err
			}
			if addrs[addr] {
				return fmt.Errorf("duplicate address %s", addr)
			}
			addrs[addr] = true
		}
		for _, n := range l.configs.Latest.Nodes {
			if _, ok := t.addrs[n.ID]; !ok {
				if addrs[n.Addr] {
					return fmt.Errorf("address %s is already used by node %s", n.Addr, n.ID)
				}
				addrs[n.Addr] = true
			}
		}
		return nil
	}()
	if err != nil {
		t.reply(fmt.Errorf("raft.ChangeAddrs: %v", err))
		return
	}

	config := l.configs.Latest.clone()
	for id, addr := range t.addrs {
		n := config.Nodes[id]
		n.Addr = addr
		config.Nodes[id] = n
	}
	l.storeConfig(t.task, config)
}

func (l *ldrShip) storeConfig(t *task, newConfig Config) {
	// append to log
	ne := NewEntry{
		entry: newConfig.encode(),
		task:  t,
	}
	l.storeEntry(ne)
	newConfig.Index, newConfig.Term = ne.index, ne.term
	l.changeConfig(newConfig)
}
