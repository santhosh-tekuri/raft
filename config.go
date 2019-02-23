package raft

import (
	"bytes"
	"fmt"
)

type NodeID string

type NodeType int8

const (
	Voter NodeType = iota
	NonVoter
	Staging
)

type Node struct {
	ID   NodeID
	Addr string
	Type NodeType
}

// -------------------------------------------------

type Config struct {
	Nodes map[NodeID]Node
	Index uint64
	Term  uint64
}

func (c Config) isVoter(id NodeID) bool {
	node, ok := c.Nodes[id]
	return ok && node.Type == Voter
}

func (c Config) quorum() int {
	voters := 0
	for _, node := range c.Nodes {
		if node.Type == Voter {
			voters++
		}
	}
	return voters/2 + 1
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
		if err := writeUint8(w, uint8(node.Type)); err != nil {
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
		suffrage, err := readUint8(r)
		if err != nil {
			return err
		}
		c.Nodes[NodeID(id)] = Node{ID: NodeID(id), Addr: addr, Type: NodeType(suffrage)}
	}
	return nil
}

// ---------------------------------------------------------

type Configs struct {
	Committed, Latest Config
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
