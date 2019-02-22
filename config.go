package raft

import (
	"bytes"
	"fmt"
)

type NodeID string

type NodeSuffrage int8

const (
	Voter NodeSuffrage = iota
	NonVoter
	Staging
)

type Node struct {
	ID       NodeID
	Addr     string
	Suffrage NodeSuffrage
}

// -------------------------------------------------

type configEntry struct {
	nodes map[NodeID]Node
	index uint64
	term  uint64
}

func (c configEntry) isVoter(id NodeID) bool {
	node, ok := c.nodes[id]
	return ok && node.Suffrage == Voter
}

func (c configEntry) quorum() int {
	voters := 0
	for _, node := range c.nodes {
		if node.Suffrage == Voter {
			voters++
		}
	}
	return voters/2 + 1
}

func (c configEntry) encode() *entry {
	w := new(bytes.Buffer)
	if err := writeUint32(w, uint32(len(c.nodes))); err != nil {
		panic(err)
	}
	for _, node := range c.nodes {
		if err := writeString(w, string(node.ID)); err != nil {
			panic(err)
		}
		if err := writeString(w, node.Addr); err != nil {
			panic(err)
		}
		if err := writeUint8(w, uint8(node.Suffrage)); err != nil {
			panic(err)
		}
	}
	return &entry{
		typ:   entryConfig,
		index: c.index,
		term:  c.term,
		data:  w.Bytes(),
	}
}

func (c *configEntry) decode(e *entry) error {
	if e.typ != entryConfig {
		return fmt.Errorf("configEntry.decode: entry type is not config")
	}
	c.index, c.term = e.index, e.term
	r := bytes.NewBuffer(e.data)
	size, err := readUint32(r)
	if err != nil {
		return err
	}
	c.nodes = make(map[NodeID]Node)
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
		c.nodes[NodeID(id)] = Node{ID: NodeID(id), Addr: addr, Suffrage: NodeSuffrage(suffrage)}
	}
	return nil
}

// ---------------------------------------------------------

type configs struct {
	committed, latest configEntry
}

func (c configs) isBootstrap() bool {
	return c.latest.index == 0
}

func (c configs) isCommitted() bool {
	return c.latest.index == c.committed.index
}
