package raft

import (
	"errors"
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
	rpcIdentity rpcType = iota
	rpcVote
	rpcAppendEntries
	rpcInstallSnap
	rpcTimeoutNow
)

func (t rpcType) isValid() bool {
	switch t {
	case rpcIdentity, rpcVote, rpcAppendEntries, rpcInstallSnap, rpcTimeoutNow:
		return true
	}
	return false
}

func (t rpcType) fromLeader() bool {
	switch t {
	case rpcAppendEntries, rpcInstallSnap, rpcTimeoutNow:
		return true
	}
	return false
}

func (t rpcType) createReq() request {
	switch t {
	case rpcIdentity:
		return &identityReq{}
	case rpcVote:
		return &voteReq{}
	case rpcAppendEntries:
		return &appendEntriesReq{}
	case rpcInstallSnap:
		return &installSnapReq{}
	case rpcTimeoutNow:
		return &timeoutNowReq{}
	}
	panic(fmt.Errorf("raft.createReq(%d)", t))
}

func (t rpcType) createResp(r *Raft, result rpcResult, err error) response {
	resp := resp{r.term, result, err}
	switch t {
	case rpcIdentity:
		return &identityResp{resp}
	case rpcVote:
		return &voteResp{resp}
	case rpcAppendEntries:
		return &appendEntriesResp{resp, r.lastLogIndex}
	case rpcInstallSnap:
		return &installSnapResp{resp}
	case rpcTimeoutNow:
		return &timeoutNowResp{resp}
	}
	panic(fmt.Errorf("raft.createResp(%d)", t))
}

type rpcResult uint8

const (
	success rpcResult = iota + 1
	idMismatch
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
	getErr() error
	setErr(error)
}

type resp struct {
	term   uint64
	result rpcResult
	err    error
}

func (resp *resp) getTerm() uint64      { return resp.term }
func (resp *resp) getResult() rpcResult { return resp.result }
func (resp *resp) getErr() error        { return resp.err }
func (resp *resp) setErr(err error)     { resp.err = err }

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
	if resp.result == unexpectedErr {
		op, err := readString(r)
		if err != nil {
			return err
		}
		errStr, err := readString(r)
		if err != nil {
			return err
		}
		resp.err = errors.New(errStr)
		if op != "" {
			resp.err = OpError{op, resp.err}
		}
	} else {
		resp.err = nil
	}
	return nil
}

func (resp *resp) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeUint8(w, uint8(resp.result)); err != nil {
		return err
	}
	if resp.result == unexpectedErr {
		op := ""
		err := resp.err
		if opErr, ok := err.(OpError); ok {
			op, err = opErr.Op, opErr.Err
		}
		if err := writeString(w, op); err != nil {
			return err
		}
		if err := writeString(w, err.Error()); err != nil {
			return err
		}
	}
	return nil
}

// ------------------------------------------------------

type identityReq struct {
	req // not used
	cid uint64
	nid uint64
}

func (req *identityReq) rpcType() rpcType { return rpcIdentity }

func (req *identityReq) decode(r io.Reader) error {
	var err error
	if req.cid, err = readUint64(r); err != nil {
		return err
	}
	req.nid, err = readUint64(r)
	return err
}

func (req *identityReq) encode(w io.Writer) error {
	if err := writeUint64(w, req.cid); err != nil {
		return nil
	}
	return writeUint64(w, req.nid)
}

// ------------------------------------------------------

type identityResp struct {
	resp
}

// ------------------------------------------------------

type voteReq struct {
	req
	lastLogIndex uint64 // index of candidate's last log entry
	lastLogTerm  uint64 // term of candidate's last log entry
}

func (req *voteReq) rpcType() rpcType { return rpcVote }

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

// ------------------------------------------------------

type appendEntriesReq struct {
	req
	prevLogIndex   uint64
	prevLogTerm    uint64
	ldrCommitIndex uint64
	numEntries     uint64
}

func (req *appendEntriesReq) rpcType() rpcType { return rpcAppendEntries }

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
	if req.ldrCommitIndex, err = readUint64(r); err != nil {
		return err
	}
	req.numEntries, err = readUint64(r)
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
	if err := writeUint64(w, req.ldrCommitIndex); err != nil {
		return err
	}
	return writeUint64(w, req.numEntries)
}

// ------------------------------------------------------

type appendEntriesResp struct {
	resp
	lastLogIndex uint64
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
}

func (req *installSnapReq) rpcType() rpcType { return rpcInstallSnap }

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
	return writeUint64(w, uint64(req.size))
}

// ------------------------------------------------------

type installSnapResp struct {
	resp
}

// ------------------------------------------------------

type timeoutNowReq struct {
	req
}

func (req *timeoutNowReq) rpcType() rpcType { return rpcTimeoutNow }

// ------------------------------------------------------

type timeoutNowResp struct {
	resp
}
