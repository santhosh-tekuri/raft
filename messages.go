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

func (t rpcType) createReq() request {
	switch t {
	case rpcVote:
		return &voteReq{}
	case rpcAppendEntries:
		return &appendEntriesReq{}
	case rpcInstallSnap:
		return &installSnapReq{}
	default:
		panic(fmt.Errorf("unknown rpcType: %d", t))
	}
}

type rpcResult uint8

const (
	success rpcResult = iota + 1
	staleTerm
	alreadyVoted
	leaderKnown
	logNotUptodate
	prevEntryNotFound
	prevTermMismatch
	readErr
	unexpectedErr
)

func (r rpcResult) String() string {
	switch r {
	case success:
		return "success"
	case staleTerm:
		return "staleTerm"
	case alreadyVoted:
		return "alreadyVoted"
	case logNotUptodate:
		return "logNotUptodate"
	case prevEntryNotFound:
		return "prevEntryNotFound"
	case prevTermMismatch:
		return "prevTermMismatch"
	case readErr:
		return "readErr"
	case unexpectedErr:
		return "unexpectedErr"
	default:
		return fmt.Sprintf("unknown(%d)", r)
	}
}

type message interface {
	getTerm() uint64
	decode(r io.Reader) error
	encode(w io.Writer) error
}

type request interface {
	message
	rpcType() rpcType
	from() uint64
}

// ------------------------------------------------------

type entryType uint8

const (
	entryBarrier entryType = iota + 1 // note: don't use zero value
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
	candidate    uint64 // candidate requesting vote
	lastLogIndex uint64 // index of candidate's last log entry
	lastLogTerm  uint64 // term of candidate's last log entry
}

func (req *voteReq) getTerm() uint64  { return req.term }
func (req *voteReq) rpcType() rpcType { return rpcVote }
func (req *voteReq) from() uint64     { return req.candidate }
func (req *voteReq) String() string {
	format := "voteReq{T%d M%d last:(%d,%d)}"
	return fmt.Sprintf(format, req.term, req.candidate, req.lastLogIndex, req.lastLogTerm)
}

func (req *voteReq) decode(r io.Reader) error {
	var err error

	if req.term, err = readUint64(r); err != nil {
		return err
	}
	if req.candidate, err = readUint64(r); err != nil {
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

func (req *voteReq) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	if err := writeUint64(w, req.candidate); err != nil {
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
	term   uint64    // currentTerm, for candidate to update itself
	result rpcResult // success means vote is granted
}

func (resp *voteResp) getTerm() uint64 { return resp.term }
func (resp *voteResp) String() string {
	return fmt.Sprintf("voteResp{T%d %s}", resp.term, resp.result)
}

func (resp *voteResp) decode(r io.Reader) error {
	var err error

	if resp.term, err = readUint64(r); err != nil {
		return err
	}
	if result, err := readUint8(r); err != nil {
		return err
	} else {
		resp.result = rpcResult(result)
	}
	return nil
}

func (resp *voteResp) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeUint8(w, uint8(resp.result)); err != nil {
		return err
	}
	return nil
}

// ------------------------------------------------------

type appendEntriesReq struct {
	term           uint64 // leader's term
	leader         uint64 // so followers can redirect clients
	prevLogIndex   uint64
	prevLogTerm    uint64
	entries        []*entry
	ldrCommitIndex uint64
}

func (req *appendEntriesReq) getTerm() uint64  { return req.term }
func (req *appendEntriesReq) rpcType() rpcType { return rpcAppendEntries }
func (req *appendEntriesReq) from() uint64     { return req.leader }
func (req *appendEntriesReq) String() string {
	format := "appendEntriesReq{T%d M%d prev:(%d,%d), #entries:%d, commit:%d}"
	return fmt.Sprintf(format, req.term, req.leader, req.prevLogIndex, req.prevLogTerm, len(req.entries), req.ldrCommitIndex)
}

func (req *appendEntriesReq) decode(r io.Reader) error {
	var err error

	if req.term, err = readUint64(r); err != nil {
		return err
	}
	if req.leader, err = readUint64(r); err != nil {
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

func (req *appendEntriesReq) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	if err := writeUint64(w, req.leader); err != nil {
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
	result       rpcResult
	lastLogIndex uint64
	// todo: what abt additional field NoRetryBackoff
}

func (resp *appendEntriesResp) getTerm() uint64 { return resp.term }
func (resp *appendEntriesResp) String() string {
	format := "appendEntriesResp{T%d %s last:%d}"
	return fmt.Sprintf(format, resp.term, resp.result, resp.lastLogIndex)
}

func (resp *appendEntriesResp) decode(r io.Reader) error {
	var err error

	if resp.term, err = readUint64(r); err != nil {
		return err
	}
	if result, err := readUint8(r); err != nil {
		return err
	} else {
		resp.result = rpcResult(result)
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
	if err := writeUint8(w, uint8(resp.result)); err != nil {
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
	leader     uint64 // so followers can redirect clients
	lastIndex  uint64 // last index in the snapshot
	lastTerm   uint64 // term of lastIndex
	lastConfig Config // last config in the snapshot
	size       int64  // size of the snapshot
	snapshot   io.Reader
}

func (req *installSnapReq) getTerm() uint64  { return req.term }
func (req *installSnapReq) rpcType() rpcType { return rpcInstallSnap }
func (req *installSnapReq) from() uint64     { return req.leader }
func (req *installSnapReq) String() string {
	format := "installSnapReq{T%d M%d last:(%d,%d), size:%d}"
	return fmt.Sprintf(format, req.term, req.leader, req.lastIndex, req.lastIndex, req.size)
}

func (req *installSnapReq) decode(r io.Reader) error {
	var err error

	if req.term, err = readUint64(r); err != nil {
		return err
	}
	if req.leader, err = readUint64(r); err != nil {
		return err
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
	req.snapshot = r
	return nil
}

func (req *installSnapReq) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	if err := writeUint64(w, req.leader); err != nil {
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
	_, err := io.CopyN(w, req.snapshot, req.size)
	return err
}

// ------------------------------------------------------

type installSnapResp struct {
	term   uint64
	result rpcResult
}

func (resp *installSnapResp) getTerm() uint64 { return resp.term }

func (resp *installSnapResp) decode(r io.Reader) error {
	var err error

	if resp.term, err = readUint64(r); err != nil {
		return err
	}
	if result, err := readUint8(r); err != nil {
		return err
	} else {
		resp.result = rpcResult(result)
	}
	return nil
}

func (resp *installSnapResp) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeUint8(w, uint8(resp.result)); err != nil {
		return err
	}
	return nil
}
