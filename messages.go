package raft

import (
	"fmt"
	"io"
)

type rpcType int

const (
	rpcVote rpcType = iota
	rpcAppendEntries
)

type message interface {
	getTerm() uint64
	decode(r io.Reader) error
	encode(w io.Writer) error
}

type entryType uint8

const (
	entryBarrier entryType = iota
	entryUpdate
	entryQuery
	entryNop
	entryConfig
)

func (t entryType) String() string {
	switch t {
	case entryUpdate:
		return "udate"
	case entryQuery:
		return "query"
	case entryNop:
		return "nop"
	case entryConfig:
		return "config"
	case entryBarrier:
		return "barrier"
	}
	return fmt.Sprintf("unknown(%d)", uint8(t))
}

type entry struct {
	index uint64
	term  uint64
	typ   entryType
	data  []byte
}

func (e *entry) decode(r io.Reader) error {
	var err error

	if e.index, err = readUint64(r); err != nil {
		return err
	}
	if e.term, err = readUint64(r); err != nil {
		return err
	}
	typ, err := readUint8(r)
	if err != nil {
		return err
	}
	e.typ = entryType(typ)
	if e.data, err = readBytes(r); err != nil {
		return err
	}
	return nil
}

func (e *entry) encode(w io.Writer) error {
	if err := writeUint64(w, e.index); err != nil {
		return err
	}
	if err := writeUint64(w, e.term); err != nil {
		return err
	}
	if err := writeUint8(w, uint8(e.typ)); err != nil {
		return err
	}
	if err := writeBytes(w, e.data); err != nil {
		return err
	}
	return nil
}

type voteRequest struct {
	term         uint64 // candidate's term
	candidateID  string // candidate requesting vote
	lastLogIndex uint64 // index of candidate's last log entry
	lastLogTerm  uint64 // term of candidate's last log entry
}

func (req *voteRequest) getTerm() uint64 {
	return req.term
}

func (req *voteRequest) decode(r io.Reader) error {
	var err error

	if req.term, err = readUint64(r); err != nil {
		return err
	}
	if req.candidateID, err = readString(r); err != nil {
		return err
	}
	if req.lastLogIndex, err = readUint64(r); err != nil {
		return err
	}
	if req.lastLogTerm, err = readUint64(r); err != nil {
		return err
	}
	return nil
}

func (req *voteRequest) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	if err := writeString(w, req.candidateID); err != nil {
		return err
	}
	if err := writeUint64(w, req.lastLogIndex); err != nil {
		return err
	}
	if err := writeUint64(w, req.lastLogTerm); err != nil {
		return err
	}
	return nil
}

type voteResponse struct {
	term    uint64 // currentTerm, for candidate to update itself
	granted bool   // true means candidate received vote
}

func (resp *voteResponse) getTerm() uint64 {
	return resp.term
}

func (resp *voteResponse) decode(r io.Reader) error {
	var err error

	if resp.term, err = readUint64(r); err != nil {
		return err
	}
	if resp.granted, err = readBool(r); err != nil {
		return err
	}
	return nil
}

func (resp *voteResponse) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeBool(w, resp.granted); err != nil {
		return err
	}
	return nil
}

type appendEntriesRequest struct {
	term           uint64
	leaderID       string
	prevLogIndex   uint64
	prevLogTerm    uint64
	entries        []*entry
	ldrCommitIndex uint64
}

func (req *appendEntriesRequest) getTerm() uint64 {
	return req.term
}

func (req *appendEntriesRequest) decode(r io.Reader) error {
	var err error

	if req.term, err = readUint64(r); err != nil {
		return err
	}
	if req.leaderID, err = readString(r); err != nil {
		return err
	}
	if req.prevLogIndex, err = readUint64(r); err != nil {
		return err
	}
	if req.prevLogTerm, err = readUint64(r); err != nil {
		return err
	}

	size, err := readUint32(r)
	if err != nil {
		return err
	}
	req.entries = make([]*entry, size)
	for i := uint32(0); i < size; i++ {
		req.entries[i] = &entry{}
		if err = req.entries[i].decode(r); err != nil {
			return err
		}
	}

	if req.ldrCommitIndex, err = readUint64(r); err != nil {
		return err
	}
	return nil
}

func (req *appendEntriesRequest) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	if err := writeString(w, req.leaderID); err != nil {
		return err
	}
	if err := writeUint64(w, req.prevLogIndex); err != nil {
		return err
	}
	if err := writeUint64(w, req.prevLogTerm); err != nil {
		return err
	}

	if err := writeUint32(w, uint32(len(req.entries))); err != nil {
		return err
	}
	for _, entry := range req.entries {
		if err := entry.encode(w); err != nil {
			return err
		}
	}

	if err := writeUint64(w, req.ldrCommitIndex); err != nil {
		return err
	}
	return nil
}

type appendEntriesResponse struct {
	term         uint64
	success      bool
	lastLogIndex uint64
	// todo: what abt additional field NoRetryBackoff
}

func (resp *appendEntriesResponse) getTerm() uint64 {
	return resp.term
}

func (resp *appendEntriesResponse) decode(r io.Reader) error {
	var err error

	if resp.term, err = readUint64(r); err != nil {
		return err
	}
	if resp.success, err = readBool(r); err != nil {
		return err
	}
	if resp.lastLogIndex, err = readUint64(r); err != nil {
		return err
	}
	return nil
}

func (resp *appendEntriesResponse) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeBool(w, resp.success); err != nil {
		return err
	}
	if err := writeUint64(w, resp.lastLogIndex); err != nil {
		return err
	}
	return nil
}
