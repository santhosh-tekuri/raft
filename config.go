package raft

import (
	"bytes"
	"fmt"
)

type NodeID string

type Node struct {
	ID      NodeID `json:"-"`
	Addr    string `json:"addr"`
	Voter   bool   `json:"voter"`
	Promote bool   `json:"promote,omitempty"`
}

// -------------------------------------------------

type Config struct {
	Nodes map[NodeID]Node `json:"nodes"`
	Index uint64          `json:"index"`
	Term  uint64          `json:"term"`
}

func (c Config) isVoter(id NodeID) bool {
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
	nodes := make(map[NodeID]Node)
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
	for _, node := range c.Nodes {
		if err := writeString(w, string(node.ID)); err != nil {
			panic(err)
		}
		if err := writeString(w, node.Addr); err != nil {
			panic(err)
		}
		if err := writeBool(w, node.Voter); err != nil {
			panic(err)
		}
		if err := writeBool(w, node.Promote); err != nil {
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
	c.Nodes = make(map[NodeID]Node)
	for ; size > 0; size-- {
		id, err := readString(r)
		if err != nil {
			return err
		}
		addr, err := readString(r)
		if err != nil {
			return err
		}
		voter, err := readBool(r)
		if err != nil {
			return err
		}
		promote, err := readBool(r)
		if err != nil {
			return err
		}
		c.Nodes[NodeID(id)] = Node{ID: NodeID(id), Addr: addr, Voter: voter, Promote: promote}
	}
	c.Index, c.Term = e.index, e.term
	return nil
}

func (c Config) validate() error {
	ids := make(map[NodeID]bool)
	addrs := make(map[string]bool)
	voters := 0
	for _, node := range c.Nodes {
		if node.ID == "" {
			return fmt.Errorf("raft: Conf.validate failed: empty node id")
		}
		if ids[node.ID] {
			return fmt.Errorf("raft: Conf.validate failed: duplicate id %s", node.ID)
		}
		ids[node.ID] = true

		if node.Addr == "" {
			return fmt.Errorf("raft: Conf.validate failed: empty address")
		}
		if addrs[node.Addr] {
			return fmt.Errorf("raft: Conf.validate failed: duplicate address %s", node.Addr)
		}
		addrs[node.Addr] = true

		if node.Voter {
			voters++
		}
	}
	if voters == 0 {
		return fmt.Errorf("raft: Conf.validate failed: no voter")
	}
	return nil
}

func (c Config) String() string {
	var voters, nonVoters []string
	for _, n := range c.Nodes {
		if n.Voter {
			voters = append(voters, fmt.Sprintf("%v[%s]", n.ID, n.Addr))
		} else if n.Promote {
			nonVoters = append(nonVoters, fmt.Sprintf("%v[%s,promote]", n.ID, n.Addr))
		} else {
			nonVoters = append(nonVoters, fmt.Sprintf("%v[%s]", n.ID, n.Addr))
		}
	}
	return fmt.Sprintf("voters: %v, nonVoters: %v", voters, nonVoters)
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

func (r *Raft) changeConfig(new Config) {
	debug(r, "changeConfig", new)
	r.configs.Committed, r.configs.Latest = r.configs.Latest, new
	r.storage.setConfigs(r.configs)
	if r.trace.ConfigChanged != nil {
		r.trace.ConfigChanged(r.liveInfo())
	}
}

func (r *Raft) commitConfig() {
	debug(r, "commitConfig", r.configs.Latest)
	r.configs.Committed = r.configs.Latest
	r.storage.setConfigs(r.configs)
	if r.trace.ConfigCommitted != nil {
		r.trace.ConfigCommitted(r.liveInfo())
	}
}

func (r *Raft) revertConfig() {
	debug(r, "revertConfig", r.configs.Committed)
	r.configs.Latest = r.configs.Committed
	r.storage.setConfigs(r.configs)
	if r.trace.ConfigReverted != nil {
		r.trace.ConfigReverted(r.liveInfo())
	}
}
