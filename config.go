// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

// Action describes the action user would like to
// perform on a node in cluster.
type Action uint8

const (
	// None means no action to be taken.
	None Action = iota

	// Promote is used to promote a nonvoter to voter.
	// Leader promotes only when the node's log sufficiently
	// caught up, to preserve availability.
	Promote

	// Demote is used to demote a voter to nonvoter.
	// Leader can demote any voter including self immediately.
	Demote

	// Remove is used to remove a node from cluster.
	//
	// Removal of voter is two step process, first it is demoted
	// to nonvoter. once the node realizes that it is nonvoter,
	// it is removed from cluster. This two step process guarantees
	// that the removed node does not disrupt the cluster after it is
	// removed. Node that removal of leader does not require two step,
	// because leader already knows that it is being removed.
	//
	// Removal of nonvoter can be done immediately.
	Remove

	// ForceRemove is similar to remove, but voter is removed immediately
	// without demoting it first. This should be used only when the node
	// has crashed and could not be restored. Note that if the
	// removed node is restored, it can disrupt the cluster.
	//
	// The library implements the solution provided in 4.2.4 to handle
	// disruptive servers.
	ForceRemove
)

func (a Action) String() string {
	switch a {
	case None:
		return "none"
	case Promote:
		return "promote"
	case Demote:
		return "demote"
	case Remove:
		return "remove"
	case ForceRemove:
		return "forceRemove"
	}
	return fmt.Sprintf("Action(%d)", a)
}

// ------------------------------------------------------------------------------

// Node represents a single node in raft configuration.
type Node struct {
	// ID uniquely identifies this node in raft cluster.
	ID uint64 `json:"-"`

	// Addr is network address that other nodes can contact.
	Addr string `json:"addr"`

	// Voter can participate in elections and its matchIndex
	// is used in advancing leader's commitIndex.
	Voter bool `json:"voter"`

	// Data can be used by application to associate some information
	// with node. Data is opaque to raft and is not interpreted.
	// For example application address
	Data string `json:"data,omitempty"`

	// Action tells the action to be taken by leader, when appropriate.
	// None action signifies that no action to be taken.
	Action Action `json:"action,omitempty"`
}

func (n Node) nextAction() Action {
	if n.Action == ForceRemove {
		return ForceRemove
	}
	if n.Voter {
		if n.Action == Demote || n.Action == Remove {
			return Demote
		}
		return None
	}
	if n.Action == Promote || n.Action == Remove {
		return n.Action
	}
	return None
}

func (n Node) encode(w io.Writer) error {
	if err := writeUint64(w, n.ID); err != nil {
		return err
	}
	if err := writeString(w, n.Addr); err != nil {
		return err
	}
	if err := writeBool(w, n.Voter); err != nil {
		return err
	}
	if err := writeString(w, n.Data); err != nil {
		return err
	}
	return writeUint8(w, uint8(n.Action))
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
	if n.Data, err = readString(r); err != nil {
		return err
	}
	action, err := readUint8(r)
	if err != nil {
		return err
	}
	n.Action = Action(action)
	return nil
}

func (n Node) validate() error {
	if n.ID == 0 {
		return errors.New("raft.Config: id must be greater than zero")
	}
	if n.Addr == "" {
		return errors.New("raft.Config: empty address")
	}
	_, sport, err := net.SplitHostPort(n.Addr)
	if err != nil {
		return fmt.Errorf("raft.Config: invalid address %s: %v", n.Addr, err)
	}
	port, err := strconv.Atoi(sport)
	if err != nil {
		return errors.New("raft.Config: port must be specified in address")
	}
	if port <= 0 {
		return errors.New("raft.Config: invalid port")
	}
	if n.Action == Promote && n.Voter {
		return errors.New("raft.Config: voter can't be promoted")
	}
	if n.Action == Demote && !n.Voter {
		return errors.New("raft.Config: nonvoter can't be demoted")
	}
	return nil
}

// ------------------------------------------------------------------------------

// Config tracks which nodes are in the cluster, whether there are
// votes, any actions to be taken on nodes.
type Config struct {
	// Nodes is the nodes in the cluster.
	// Key is the node ID.
	Nodes map[uint64]Node `json:"nodes"`

	// Index is the log index of this config.
	Index uint64 `json:"index"`

	// Term in which the config is created.
	Term uint64 `json:"term"`
}

func (c Config) isBootstrapped() bool {
	return c.Index > 0
}

func (c Config) isStable() bool {
	for _, n := range c.Nodes {
		if n.Action != None {
			return false
		}
	}
	return true
}

func (c Config) nodeForAddr(addr string) (Node, bool) {
	for _, n := range c.Nodes {
		if n.Addr == addr {
			return n, true
		}
	}
	return Node{}, false
}

func (c Config) isVoter(id uint64) bool {
	n, ok := c.Nodes[id]
	return ok && n.Voter
}

func (c Config) numVoters() int {
	voters := 0
	for _, n := range c.Nodes {
		if n.Voter {
			voters++
		}
	}
	return voters
}

func (c Config) quorum() int {
	return c.numVoters()/2 + 1
}

// AddVoter adds given node as voter.
//
// This call fails if config is not bootstrap.
func (c *Config) AddVoter(id uint64, addr string) error {
	if c.isBootstrapped() {
		return fmt.Errorf("raft.Config: voter cannot be added in bootstrapped config")
	}
	return c.addNode(Node{ID: id, Addr: addr, Voter: true})
}

// AddNonvoter adds given node as nonvoter.
//
// Voters can't be directly added to cluster. They must be added as
// nonvoter with promote turned on. once the node's log catches up,
// leader promotes it to voter.
func (c *Config) AddNonvoter(id uint64, addr string, promote bool) error {
	action := None
	if promote {
		action = Promote
	}
	return c.addNode(Node{ID: id, Addr: addr, Action: action})
}

func (c *Config) addNode(n Node) error {
	if err := n.validate(); err != nil {
		return err
	}
	if _, ok := c.Nodes[n.ID]; ok {
		return fmt.Errorf("raft.Config: node %d already exists", n.ID)
	}
	c.Nodes[n.ID] = n
	return nil
}

// SetAction sets the action to be token on given node.
func (c *Config) SetAction(id uint64, action Action) error {
	n, ok := c.Nodes[id]
	if !ok {
		return fmt.Errorf("raft.Config: node %d not found", id)
	}
	n.Action = action
	if err := n.validate(); err != nil {
		return err
	}
	c.Nodes[id] = n
	return nil
}

// SetAddr changes address of given node.
//
// If you have set Options.Resolver, the address resolved
// by Resolver still takes precedence.
func (c *Config) SetAddr(id uint64, addr string) error {
	n, ok := c.Nodes[id]
	if !ok {
		return fmt.Errorf("raft.Config: node %d not found", id)
	}
	n.Addr = addr
	if err := n.validate(); err != nil {
		return err
	}
	cn, ok := c.nodeForAddr(addr)
	if ok && cn.ID != id {
		return fmt.Errorf("raft.Config: address %s is used by node %d", addr, cn.ID)
	}
	c.Nodes[id] = n
	return nil
}

// SetData changes data associated with given node.
func (c *Config) SetData(id uint64, data string) error {
	n, ok := c.Nodes[id]
	if !ok {
		return fmt.Errorf("raft.Config: node %d not found", id)
	}
	n.Data = data
	c.Nodes[id] = n
	return nil
}

func (c Config) clone() Config {
	nodes := make(map[uint64]Node)
	for id, n := range c.Nodes {
		nodes[id] = n
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
	return nil
}

func (c Config) validate() error {
	addrs := make(map[string]bool)
	for id, n := range c.Nodes {
		if err := n.validate(); err != nil {
			return err
		}
		if id != n.ID {
			return fmt.Errorf("raft.Config: id mismatch for node %d", n.ID)
		}
		if addrs[n.Addr] {
			return fmt.Errorf("raft.Config: duplicate address %s", n.Addr)
		}
		addrs[n.Addr] = true
	}
	if c.numVoters() == 0 {
		return errors.New("raft.Config: zero voters")
	}
	return nil
}

func (c Config) String() string {
	var voters, nonvoters []string
	for _, n := range c.Nodes {
		s := fmt.Sprintf("%d,%s", n.ID, n.Addr)
		if n.Action != None {
			s = fmt.Sprintf("%s,%s", s, n.Action)
		}
		if n.Voter {
			voters = append(voters, s)
		} else {
			nonvoters = append(nonvoters, s)
		}
	}
	return fmt.Sprintf("Config{index: %d, voters: %v, nonvoters: %v}", c.Index, voters, nonvoters)
}

// ------------------------------------------------------------------------------

// Configs captures the committed and latest config.
// If committed and latest are same, it means latest config
// is committed.
type Configs struct {
	Committed Config `json:"committed"`
	Latest    Config `json:"latest"`
}

func (c Configs) clone() Configs {
	c.Committed = c.Committed.clone()
	c.Latest = c.Latest.clone()
	return c
}

// IsBootstrapped returns true if the server is
// already bootstrapped.
func (c Configs) IsBootstrapped() bool {
	return c.Latest.isBootstrapped()
}

// IsCommitted return true, if current config is
// committed.
func (c Configs) IsCommitted() bool {
	return c.Latest.Index == c.Committed.Index
}

// IsStable return true, if current config is committed
// and no further actions are pending in config.
func (c Configs) IsStable() bool {
	return c.IsCommitted() && c.Latest.isStable()
}

// ---------------------------------------------------------

func (r *Raft) bootstrap(t changeConfig) {
	if r.configs.IsBootstrapped() {
		t.reply(notLeaderError(r, false))
		return
	}
	if err := t.newConf.validate(); err != nil {
		t.reply(err)
		return
	}
	self, ok := t.newConf.Nodes[r.nid]
	if !ok {
		t.reply(fmt.Errorf("raft.bootstrap: self %d does not exist", r.nid))
		return
	}
	if !self.Voter {
		t.reply(fmt.Errorf("raft.bootstrap: self %d must be voter", r.nid))
		return
	}
	if !t.newConf.isStable() {
		t.reply(fmt.Errorf("raft.bootstrap: non-stable config"))
		return
	}

	t.newConf.Index, t.newConf.Term = 1, 1
	if trace {
		println(r, "bootstrapping", t.newConf)
	}
	if err := r.storage.bootstrap(t.newConf); err != nil {
		t.reply(err)
		return
	}
	r.changeConfig(t.newConf)
	t.reply(nil)
	r.setState(Candidate)
}

// ---------------------------------------------------------

func (l *leader) setCommitIndex(index uint64) {
	if trace {
		println(l, "log.Commit", index)
	}
	l.storage.commitLog(index)
	if l.commitIndex < l.startIndex && index >= l.startIndex {
		l.logger.Info("ready for commit")
		if tracer.commitReady != nil {
			tracer.commitReady(l.Raft)
		}
	}
	configCommitted := l.Raft.setCommitIndex(index)
	if configCommitted {
		if l.configs.IsStable() {
			if trace {
				println(l, "stableConfig")
			}
			l.logger.Info("config is stable")
			for _, t := range l.waitStable {
				t.reply(l.configs.Latest)
			}
			l.waitStable = nil
		} else {
			l.checkConfigActions(nil, l.configs.Latest)
		}
	}
}

func (r *Raft) setCommitIndex(index uint64) (configCommitted bool) {
	r.commitIndex = index
	if trace {
		println(r, "commitIndex", r.commitIndex)
	}
	if !r.configs.IsCommitted() && r.configs.Latest.Index <= r.commitIndex {
		r.commitConfig()
		configCommitted = true
		if r.state == Leader && !r.configs.Latest.isVoter(r.nid) {
			// if we are no longer voter after this config is committed,
			// then what is the point of accepting fsm entries from user ????
			if trace {
				println(r, "leader -> follower notVoter")
			}
			r.setState(Follower)
			r.setLeader(0)
		}
		if r.shutdownOnRemove {
			if _, ok := r.configs.Latest.Nodes[r.nid]; !ok {
				r.doClose(ErrNodeRemoved)
			}
		}
	}
	return
}

func (l *leader) changeConfig(config Config) {
	l.node = config.Nodes[l.nid]
	l.numVoters = l.configs.Latest.numVoters()
	l.Raft.changeConfig(config)

	// remove repls
	for id, repl := range l.repls {
		if _, ok := config.Nodes[id]; !ok {
			repl.status.removed = true
			close(repl.stopCh)
			delete(l.repls, id)
		}
	}

	// add new repls
	for id, n := range config.Nodes {
		if id != l.nid {
			if repl, ok := l.repls[id]; !ok {
				l.addReplication(n)
			} else {
				repl.status.node = n
			}
		}
	}
	l.checkConfigActions(nil, l.configs.Latest)
}

func (r *Raft) changeConfig(config Config) {
	if trace {
		println(r, "changeConfig", config)
	}
	if r.leader != 0 && !config.isVoter(r.leader) { // leader removed
		r.setLeader(0) // for faster election

	}
	r.configs.Committed = r.configs.Latest
	r.setLatest(config)
	if r.configs.Latest.Index == 1 {
		r.logger.Info("bootstrapped with", r.configs.Latest)
	} else {
		r.logger.Info("changed to", r.configs.Latest)
	}
	if tracer.configChanged != nil {
		tracer.configChanged(r)
	}
}

func (r *Raft) commitConfig() {
	if trace {
		println(r, "commitConfig", r.configs.Latest)
	}
	if r.leader != 0 && !r.configs.Latest.isVoter(r.leader) { // leader removed
		r.setLeader(0) // for faster election
	}
	r.configs.Committed = r.configs.Latest
	r.logger.Info("committed", r.configs.Latest)
	if tracer.configCommitted != nil {
		tracer.configCommitted(r)
	}
}

func (r *Raft) revertConfig() {
	if trace {
		println(r, "revertConfig", r.configs.Committed)
	}
	r.setLatest(r.configs.Committed)
	r.logger.Info("reverted to", r.configs.Latest)
	if tracer.configReverted != nil {
		tracer.configReverted(r)
	}
}

func (r *Raft) setLatest(config Config) {
	r.configs.Latest = config
	r.resolver.update(config)
}
