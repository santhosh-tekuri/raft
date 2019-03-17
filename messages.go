package raft

import (
	"fmt"
	"io"
)

type entryType uint8

const (
	entryBarrier entryType = iota + 1
	entryUpdate
	entryRead
	entryNop
	entryConfig
)

func (t entryType) String() string {
	switch t {
	case entryBarrier:
		return "barrier"
	case entryUpdate:
		return "update"
	case entryRead:
		return "read"
	case entryNop:
		return "nop"
	case entryConfig:
		return "config"
	}
	return fmt.Sprintf("entryType(%d)", uint8(t))
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
	e.data, err = readBytes(r)
	return err
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
	return writeBytes(w, e.data)
}

// ------------------------------------------------------

type rpcType int

const (
	rpcVote rpcType = iota
	rpcAppendEntries
	rpcInstallSnap
	rpcTimeoutNow
)

func (t rpcType) fromLeader() bool {
	return t != rpcVote
}

func (t rpcType) createReq() request {
	switch t {
	case rpcVote:
		return &voteReq{}
	case rpcAppendEntries:
		return &appendEntriesReq{}
	case rpcInstallSnap:
		return &installSnapReq{}
	case rpcTimeoutNow:
		return &timeoutNowReq{}
	}
	panic(fmt.Errorf("raft.createReq: unknown rpcType %d", t))
}

func (t rpcType) createResp(r *Raft, result rpcResult) response {
	resp := resp{r.term, result}
	switch t {
	case rpcVote:
		return &voteResp{resp}
	case rpcAppendEntries:
		return &appendEntriesResp{resp, r.lastLogIndex}
	case rpcInstallSnap:
		return &installSnapResp{resp}
	case rpcTimeoutNow:
		return &timeoutNowResp{resp}
	}
	panic(fmt.Errorf("raft.createResp: unknown rpcType %d", t))
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
	case leaderKnown:
		return "leaderKnown"
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
	}
	return fmt.Sprintf("rpcResult(%d)", r)
}

type message interface {
	getTerm() uint64
	decode(r io.Reader) error
	encode(w io.Writer) error
}

// ------------------------------------------------------

type request interface {
	rpcType() rpcType
	message
	from() uint64
}

type req struct {
	term uint64
	src  uint64
}

func (req *req) getTerm() uint64 { return req.term }
func (req *req) from() uint64    { return req.src }
func (req *req) decode(r io.Reader) error {
	var err error
	if req.term, err = readUint64(r); err != nil {
		return err
	}
	req.src, err = readUint64(r)
	return err
}

func (req *req) encode(w io.Writer) error {
	if err := writeUint64(w, req.term); err != nil {
		return err
	}
	return writeUint64(w, req.src)
}

// ------------------------------------------------------

type response interface {
	message
	getResult() rpcResult
}

type resp struct {
	term   uint64
	result rpcResult
}

func (resp *resp) getTerm() uint64      { return resp.term }
func (resp *resp) getResult() rpcResult { return resp.result }

func (resp *resp) decode(r io.Reader) error {
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

func (resp *resp) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	return writeUint8(w, uint8(resp.result))
}

// ------------------------------------------------------

type voteReq struct {
	req
	lastLogIndex uint64 // index of candidate's last log entry
	lastLogTerm  uint64 // term of candidate's last log entry
}

func (req *voteReq) rpcType() rpcType { return rpcVote }
func (req *voteReq) String() string {
	format := "voteReq{T%d M%d last:(%d,%d)}"
	return fmt.Sprintf(format, req.term, req.src, req.lastLogIndex, req.lastLogTerm)
}

func (req *voteReq) decode(r io.Reader) error {
	var err error
	if err = req.req.decode(r); err != nil {
		return err
	}
	if req.lastLogIndex, err = readUint64(r); err != nil {
		return err
	}
	req.lastLogTerm, err = readUint64(r)
	return err
}

func (req *voteReq) encode(w io.Writer) error {
	if err := req.req.encode(w); err != nil {
		return err
	}
	if err := writeUint64(w, req.lastLogIndex); err != nil {
		return err
	}
	return writeUint64(w, req.lastLogTerm)
}

// ------------------------------------------------------

type voteResp struct {
	resp
}

func (resp *voteResp) String() string {
	return fmt.Sprintf("voteResp{T%d %s}", resp.term, resp.result)
}

// ------------------------------------------------------

type appendEntriesReq struct {
	req
	prevLogIndex   uint64
	prevLogTerm    uint64
	entries        []*entry
	ldrCommitIndex uint64
}

func (req *appendEntriesReq) rpcType() rpcType { return rpcAppendEntries }
func (req *appendEntriesReq) String() string {
	format := "appendEntriesReq{T%d M%d prev:(%d,%d), #entries:%d, commit:%d}"
	return fmt.Sprintf(format, req.term, req.src, req.prevLogIndex, req.prevLogTerm, len(req.entries), req.ldrCommitIndex)
}

func (req *appendEntriesReq) decode(r io.Reader) error {
	var err error
	if err = req.req.decode(r); err != nil {
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

	req.ldrCommitIndex, err = readUint64(r)
	return err
}

func (req *appendEntriesReq) encode(w io.Writer) error {
	if err := req.req.encode(w); err != nil {
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

	return writeUint64(w, req.ldrCommitIndex)
}

// ------------------------------------------------------

type appendEntriesResp struct {
	resp
	lastLogIndex uint64
}

func (resp *appendEntriesResp) String() string {
	format := "appendEntriesResp{T%d %s last:%d}"
	return fmt.Sprintf(format, resp.term, resp.result, resp.lastLogIndex)
}

func (resp *appendEntriesResp) decode(r io.Reader) error {
	var err error
	if err = resp.resp.decode(r); err != nil {
		return err
	}
	resp.lastLogIndex, err = readUint64(r)
	return err
}

func (resp *appendEntriesResp) encode(w io.Writer) error {
	if err := resp.resp.encode(w); err != nil {
		return err
	}
	return writeUint64(w, resp.lastLogIndex)
}

// ------------------------------------------------------

type installSnapReq struct {
	req
	lastIndex  uint64 // last index in the snapshot
	lastTerm   uint64 // term of lastIndex
	lastConfig Config // last config in the snapshot
	size       int64  // size of the snapshot
	snapshot   io.Reader
}

func (req *installSnapReq) rpcType() rpcType { return rpcInstallSnap }
func (req *installSnapReq) String() string {
	format := "installSnapReq{T%d M%d last:(%d,%d), size:%d}"
	return fmt.Sprintf(format, req.term, req.src, req.lastIndex, req.lastIndex, req.size)
}

func (req *installSnapReq) decode(r io.Reader) error {
	var err error
	if err = req.req.decode(r); err != nil {
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
	if err := req.req.encode(w); err != nil {
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
	resp
}

func (resp *installSnapResp) String() string {
	return fmt.Sprintf("installSnapResp{T%d %s}", resp.term, resp.result)
}

// ------------------------------------------------------

type timeoutNowReq struct {
	req
}

func (req *timeoutNowReq) rpcType() rpcType { return rpcTimeoutNow }
func (req *timeoutNowReq) String() string {
	return fmt.Sprintf("timeoutNowReq{T%d M%d}", req.term, req.src)
}

// ------------------------------------------------------

type timeoutNowResp struct {
	resp
}

func (resp *timeoutNowResp) String() string {
	return fmt.Sprintf("timeoutNowResp{T%d %s}", resp.term, resp.result)
}
