package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

// Node represents a single node in raft configuration.
type Node struct {
	// ID is a unique string identifying this node for all time.
	ID uint64 `json:"-"`

	// Addr is its network address that other nodes can contact.
	Addr string `json:"addr"`

	// Voter determines whether it can participate in elections and
	// its matchIndex is used in advancing leader's commitIndex.
	Voter bool `json:"voter"`

	// Promote determines whether this node should be promoted to
	// voter. Leader promotes such node, when it has caught
	// up with rest of cluster.
	//
	// This is applicable only if it is nonvoter.
	Promote bool `json:"promote,omitempty"`
}

func (n Node) promote() bool {
	if n.Voter {
		return false
	}
	return n.Promote
}

func (n Node) encode(w io.Writer) *entry {
	if err := writeUint64(w, n.ID); err != nil {
		panic(err)
	}
	if err := writeString(w, n.Addr); err != nil {
		panic(err)
	}
	if err := writeBool(w, n.Voter); err != nil {
		panic(err)
	}
	if err := writeBool(w, n.promote()); err != nil {
		panic(err)
	}
	return nil
}

func (n *Node) decode(r io.Reader) error {
	var err error
	if n.ID, err = readUint64(r); err != nil {
		return err
	}
	if n.Addr, err = readString(r); err != nil {
		return err
	}
	if n.Voter, err = readBool(r); err != nil {
		return err
	}
	if n.Promote, err = readBool(r); err != nil {
		return err
	}
	return nil
}

func (n Node) validate() error {
	if n.ID == 0 {
		return errors.New("id must be greater than zero")
	}
	if n.Addr == "" {
		return errors.New("empty address")
	}
	_, sport, err := net.SplitHostPort(n.Addr)
	if err != nil {
		return fmt.Errorf("invalid address %s: %v", n.Addr, err)
	}
	port, err := strconv.Atoi(sport)
	if err != nil {
		return errors.New("port must be specified in address")
	}
	if port <= 0 {
		return errors.New("invalid port")
	}
	return nil
}

// -------------------------------------------------

type Config struct {
	Nodes map[uint64]Node `json:"nodes"`
	Index uint64          `json:"index"`
	Term  uint64          `json:"term"`
}

func (c Config) nodeForAddr(addr string) (Node, bool) {
	for _, node := range c.Nodes {
		if node.Addr == addr {
			return node, true
		}
	}
	return Node{}, false
}

func (c Config) isVoter(id uint64) bool {
	node, ok := c.Nodes[id]
	return ok && node.Voter
}

func (c Config) numVoters() int {
	voters := 0
	for _, node := range c.Nodes {
		if node.Voter {
			voters++
		}
	}
	return voters
}

func (c Config) quorum() int {
	return c.numVoters()/2 + 1
}

func (c Config) clone() Config {
	nodes := make(map[uint64]Node)
	for id, node := range c.Nodes {
		nodes[id] = node
	}
	c.Nodes = nodes
	return c
}

func (c Config) encode() *entry {
	w := new(bytes.Buffer)
	if err := writeUint32(w, uint32(len(c.Nodes))); err != nil {
		panic(err)
	}
	for _, n := range c.Nodes {
		if err := n.encode(w); err != nil {
			panic(err)
		}
	}
	return &entry{
		typ:   entryConfig,
		index: c.Index,
		term:  c.Term,
		data:  w.Bytes(),
	}
}

func (c *Config) decode(e *entry) error {
	if e.typ != entryConfig {
		return fmt.Errorf("raft: expected entryConfig in Config.decode")
	}
	c.Index, c.Term = e.index, e.term
	r := bytes.NewBuffer(e.data)
	size, err := readUint32(r)
	if err != nil {
		return err
	}
	c.Nodes = make(map[uint64]Node)
	for ; size > 0; size-- {
		n := Node{}
		if err := n.decode(r); err != nil {
			return err
		}
		c.Nodes[n.ID] = n
	}
	c.Index, c.Term = e.index, e.term
	return nil
}

func (c Config) validate() error {
	addrs := make(map[string]bool)
	for id, node := range c.Nodes {
		if err := node.validate(); err != nil {
			return err
		}
		if id != node.ID {
			return fmt.Errorf("id mismatch for node %d", node.ID)
		}
		if addrs[node.Addr] {
			return fmt.Errorf("duplicate address %s", node.Addr)
		}
		addrs[node.Addr] = true
	}
	if c.numVoters() == 0 {
		return errors.New("zero voters")
	}
	return nil
}

func (c Config) String() string {
	var voters, nonvoters []string
	for _, n := range c.Nodes {
		if n.Voter {
			voters = append(voters, fmt.Sprintf("%v(%s)", n.ID, n.Addr))
		} else if n.promote() {
			nonvoters = append(nonvoters, fmt.Sprintf("%v(%s,promote)", n.ID, n.Addr))
		} else {
			nonvoters = append(nonvoters, fmt.Sprintf("%v(%s)", n.ID, n.Addr))
		}
	}
	return fmt.Sprintf("index: %d, voters: %v, nonvoters: %v", c.Index, voters, nonvoters)
}

// ---------------------------------------------------------

type Configs struct {
	Committed Config `json:"committed"`
	Latest    Config `json:"latest"`
}

func (c Configs) clone() Configs {
	c.Committed = c.Committed.clone()
	c.Latest = c.Latest.clone()
	return c
}

func (c Configs) IsBootstrap() bool {
	return c.Latest.Index == 0
}

func (c Configs) IsCommitted() bool {
	return c.Latest.Index == c.Committed.Index
}

// ---------------------------------------------------------

func (r *Raft) bootstrap(t bootstrap) {
	if !r.configs.IsBootstrap() {
		t.reply(ErrAlreadyBootstrapped)
		return
	}
	config := Config{Nodes: t.nodes, Index: 1, Term: 1}
	if err := config.validate(); err != nil {
		t.reply(fmt.Errorf("raft.bootstrap: invalid config: %v", err))
		return
	}
	self, ok := config.Nodes[r.id]
	if !ok {
		t.reply(fmt.Errorf("raft.bootstrap: invalid config: self %d does not exist", r.id))
		return
	}
	if !self.Voter {
		t.reply(fmt.Errorf("raft.bootstrap: invalid config: self %d must be voter", r.id))
		return
	}

	debug(r, "bootstrapping....")
	if err := r.storage.bootstrap(config); err != nil {
		t.reply(err)
		return
	}
	r.changeConfig(config)
	t.reply(nil)
	r.setState(Candidate)
}

func (l *ldrShip) changeConfig(t changeConfig) {
	if !l.configs.IsCommitted() {
		t.reply(InProgressError("configChange"))
		return
	}
	if l.commitIndex < l.startIndex {
		t.reply(ErrNotCommitReady)
		return
	}
	if t.newConf.Index != l.configs.Latest.Index {
		t.reply(ErrConfigChanged)
		return
	}
	if err := t.newConf.validate(); err != nil {
		t.reply(fmt.Errorf("raft.changeConfig: %v", err))
		return
	}

	voters := func(c Config) map[uint64]bool {
		m := make(map[uint64]bool)
		for id, n := range c.Nodes {
			if n.Voter {
				m[id] = true
			}
		}
		return m
	}

	curVoters, newVoters := voters(l.configs.Latest), voters(t.newConf)

	// remove common voters
	for curVoter := range curVoters {
		if newVoters[curVoter] {
			delete(curVoters, curVoter)
			delete(newVoters, curVoter)
		}
	}

	if len(newVoters) > 0 {
		t.reply(fmt.Errorf("raft.changeConfig: new voters found. please add them as nonvoters with promote flag"))
		return
	}
	if len(curVoters) > 1 {
		t.reply(errors.New("raft.changeConfig: only one voter can lose its voting right"))
		return
	}
	l.doChangeConfig(t.task, t.newConf)
}

func (l *ldrShip) doChangeConfig(t *task, config Config) {
	// store config, and update our latest config
	err := l.storeEntry(NewEntry{
		entry: config.encode(),
		task:  t,
	})
	if err != nil {
		return
	}

	// remove flrs
	for id, f := range l.flrs {
		if _, ok := config.Nodes[id]; !ok {
			close(f.stopCh)
			delete(l.flrs, id)
		}
	}

	// add new flrs
	for id, node := range config.Nodes {
		if id == l.id {
			continue
		}
		if _, ok := l.flrs[id]; !ok {
			l.addFlr(node)
		}
	}
}

// ---------------------------------------------------------

func (r *Raft) setCommitIndex(index uint64) {
	r.commitIndex = index
	debug(r, "commitIndex", r.commitIndex)
	if !r.configs.IsCommitted() && r.configs.Latest.Index <= r.commitIndex {
		r.commitConfig()
		if r.state == Leader && !r.configs.Latest.isVoter(r.id) {
			// if we are no longer voter after this config is committed,
			// then what is the point of accepting fsm entries from user ????
			debug(r, "leader -> follower notVoter")
			r.setState(Follower)
			r.setLeader(0)
		}
		// todo: we can provide option to shutdown
		//       if it is no longer part of new config
	}
}

func (r *Raft) changeConfig(new Config) {
	debug(r, "changeConfig", new)
	r.configs.Committed = r.configs.Latest
	r.setLatest(new)
	if r.trace.ConfigChanged != nil {
		r.trace.ConfigChanged(r.liveInfo())
	}
}

func (r *Raft) commitConfig() {
	debug(r, "commitConfig", r.configs.Latest)
	r.configs.Committed = r.configs.Latest
	if r.trace.ConfigCommitted != nil {
		r.trace.ConfigCommitted(r.liveInfo())
	}
}

func (r *Raft) revertConfig() {
	debug(r, "revertConfig", r.configs.Committed)
	r.setLatest(r.configs.Committed)
	if r.trace.ConfigReverted != nil {
		r.trace.ConfigReverted(r.liveInfo())
	}
}

func (r *Raft) setLatest(config Config) {
	r.configs.Latest = config
	r.resolver.update(config)
}
