package raft

import (
	"fmt"
	"io"
)

type rpcType int

const (
	rpcVote rpcType = iota
	rpcAppendEntries
	rpcInstallSnap
)

type message interface {
	getTerm() uint64
	decode(r io.Reader) error
	encode(w io.Writer) error
}

// ------------------------------------------------------

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
	case entryBarrier:
		return "barrier"
	case entryUpdate:
		return "update"
	case entryQuery:
		return "query"
	case entryNop:
		return "nop"
	case entryConfig:
		return "config"
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

// ------------------------------------------------------

type voteReq struct {
	term         uint64 // candidate's term
	candidate    ID     // candidate requesting vote
	lastLogIndex uint64 // index of candidate's last log entry
	lastLogTerm  uint64 // term of candidate's last log entry
}

func (req *voteReq) getTerm() uint64 { return req.term }

func (req *voteReq) decode(r io.Reader) error {
	var err error

	if req.term, err = readUint64(r); err != nil {
		return err
	}
	if s, err := readString(r); err != nil {
		return err
	} else {
		req.candidate = ID(s)
	}
	if req.lastLogIndex, err = readUint64(r); err != nil {
		return err
	}
	if req.lastLogTerm, err = readUint64(r); err != nil {
		return err
	}
	return nil
}

func (req *voteReq) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	if err := writeString(w, string(req.candidate)); err != nil {
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

// ------------------------------------------------------

type voteResp struct {
	term    uint64 // currentTerm, for candidate to update itself
	granted bool   // true means candidate received vote
}

func (resp *voteResp) getTerm() uint64 { return resp.term }

func (resp *voteResp) decode(r io.Reader) error {
	var err error

	if resp.term, err = readUint64(r); err != nil {
		return err
	}
	if resp.granted, err = readBool(r); err != nil {
		return err
	}
	return nil
}

func (resp *voteResp) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeBool(w, resp.granted); err != nil {
		return err
	}
	return nil
}

// ------------------------------------------------------

type appendEntriesReq struct {
	term           uint64 // leader's term
	leader         ID     // so followers can redirect clients
	prevLogIndex   uint64
	prevLogTerm    uint64
	entries        []*entry
	ldrCommitIndex uint64
}

func (req *appendEntriesReq) getTerm() uint64 { return req.term }

func (req *appendEntriesReq) decode(r io.Reader) error {
	var err error

	if req.term, err = readUint64(r); err != nil {
		return err
	}
	if ldr, err := readString(r); err != nil {
		return err
	} else {
		req.leader = ID(ldr)
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

func (req *appendEntriesReq) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	if err := writeString(w, string(req.leader)); err != nil {
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

// ------------------------------------------------------

type appendEntriesResp struct {
	term         uint64
	success      bool
	lastLogIndex uint64
	// todo: what abt additional field NoRetryBackoff
}

func (resp *appendEntriesResp) getTerm() uint64 { return resp.term }

func (resp *appendEntriesResp) decode(r io.Reader) error {
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

func (resp *appendEntriesResp) encode(w io.Writer) error {
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

// ------------------------------------------------------

type installSnapReq struct {
	term       uint64 // leader's term
	leader     ID     // so followers can redirect clients
	lastIndex  uint64 // last index in the snapshot
	lastTerm   uint64 // term of lastIndex
	lastConfig Config // last config in the snapshot
	size       int64  // size of the snapshot
}

func (req *installSnapReq) getTerm() uint64 { return req.term }

func (req *installSnapReq) decode(r io.Reader) error {
	var err error

	if req.term, err = readUint64(r); err != nil {
		return err
	}
	if ldr, err := readString(r); err != nil {
		return err
	} else {
		req.leader = ID(ldr)
	}
	if req.lastIndex, err = readUint64(r); err != nil {
		return err
	}
	if req.lastTerm, err = readUint64(r); err != nil {
		return err
	}

	e := &entry{}
	if err = e.decode(r); err != nil {
		return err
	}
	if err = req.lastConfig.decode(e); err != nil {
		return err
	}

	if size, err := readUint64(r); err != nil {
		return err
	} else {
		req.size = int64(size)
	}
	return nil
}

func (req *installSnapReq) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	if err := writeString(w, string(req.leader)); err != nil {
		return err
	}
	if err := writeUint64(w, req.lastIndex); err != nil {
		return err
	}
	if err := writeUint64(w, req.lastTerm); err != nil {
		return err
	}

	e := req.lastConfig.encode()
	if err := e.encode(w); err != nil {
		return err
	}

	if err := writeUint64(w, uint64(req.size)); err != nil {
		return err
	}
	return nil
}

// ------------------------------------------------------

type installSnapResp struct {
	term    uint64
	success bool
}

func (resp *installSnapResp) getTerm() uint64 { return resp.term }

func (resp *installSnapResp) decode(r io.Reader) error {
	var err error

	if resp.term, err = readUint64(r); err != nil {
		return err
	}
	if resp.success, err = readBool(r); err != nil {
		return err
	}
	return nil
}

func (resp *installSnapResp) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeBool(w, resp.success); err != nil {
		return err
	}
	return nil
}
