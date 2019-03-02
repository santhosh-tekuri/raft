package raft

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
)

// ID is a unique string identifying a node for all time.
type ID string

// Node represents a single node in raft configuration.
type Node struct {
	// ID is a unique string identifying this node for all time.
	ID ID `json:"-"`

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

func (n Node) validate() error {
	if n.ID == "" {
		return errors.New("empty node id")
	}
	return validateAddr(n.Addr)
}

func validateAddr(addr string) error {
	if addr == "" {
		return errors.New("empty address")
	}
	_, sport, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address %s: %v", addr, err)
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
	Nodes map[ID]Node `json:"nodes"`
	Index uint64      `json:"index"`
	Term  uint64      `json:"term"`
}

func (c Config) nodeForAddr(addr string) (Node, bool) {
	for _, node := range c.Nodes {
		if node.Addr == addr {
			return node, true
		}
	}
	return Node{}, false
}

func (c Config) isVoter(id ID) bool {
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
	nodes := make(map[ID]Node)
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
	c.Nodes = make(map[ID]Node)
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
		c.Nodes[ID(id)] = Node{ID: ID(id), Addr: addr, Voter: voter, Promote: promote}
	}
	c.Index, c.Term = e.index, e.term
	return nil
}

func (c Config) String() string {
	var voters, nonvoters []string
	for _, n := range c.Nodes {
		if n.Voter {
			voters = append(voters, fmt.Sprintf("%v[%s]", n.ID, n.Addr))
		} else if n.Promote {
			nonvoters = append(nonvoters, fmt.Sprintf("%v[%s,promote]", n.ID, n.Addr))
		} else {
			nonvoters = append(nonvoters, fmt.Sprintf("%v[%s]", n.ID, n.Addr))
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

func (r *Raft) changeConfig(new Config) {
	debug(r, "changeConfig", new)
	r.configs.Committed = r.configs.Latest
	r.setLatest(new)
	r.storage.saveConfigs()
	if r.trace.ConfigChanged != nil {
		r.trace.ConfigChanged(r.liveInfo())
	}
}

func (r *Raft) commitConfig() {
	debug(r, "commitConfig", r.configs.Latest)
	r.configs.Committed = r.configs.Latest
	r.storage.saveConfigs()
	if r.trace.ConfigCommitted != nil {
		r.trace.ConfigCommitted(r.liveInfo())
	}
}

func (r *Raft) revertConfig() {
	debug(r, "revertConfig", r.configs.Committed)
	r.setLatest(r.configs.Committed)
	r.storage.saveConfigs()
	if r.trace.ConfigReverted != nil {
		r.trace.ConfigReverted(r.liveInfo())
	}
}

func (r *Raft) setLatest(config Config) {
	r.configs.Latest = config
	r.resolver.update(config)
}
