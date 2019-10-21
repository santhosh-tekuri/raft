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
	"bufio"
	"errors"
	"fmt"
	"io"
)

type entryType uint8

const (
	entryBarrier entryType = iota + 1
	entryUpdate
	entryRead
	entryDirtyRead
	entryNop
	entryConfig
)

type entry struct {
	index uint64
	term  uint64
	typ   entryType
	data  []byte
}

func (e *entry) isLogEntry() bool {
	switch e.typ {
	case entryRead, entryDirtyRead, entryBarrier:
		return false
	default:
		return true
	}
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

// tells whether entry is completely in buffer
func isEntryBuffered(r *bufio.Reader) bool {
	headerLen := 8 + 8 + 1 + 4 // index+term+typ+len(data)
	buffered := r.Buffered()
	if buffered < headerLen {
		return false
	}
	b, err := r.Peek(headerLen)
	assert(err == nil)
	dataLen := byteOrder.Uint32(b[headerLen-4:])
	return buffered >= headerLen+int(dataLen)
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
		return &appendReq{}
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
		return &appendResp{resp, r.lastLogIndex}
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
	identityMismatch
	staleTerm
	alreadyVoted
	leaderKnown
	logNotUptodate
	prevEntryNotFound
	prevTermMismatch
	nonVoter
	readErr
	unexpectedErr
)

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
	result, err := readUint8(r)
	if err != nil {
		return err
	}
	resp.result = rpcResult(result)
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
	if err = req.req.decode(r); err != nil {
		return err
	}
	if req.cid, err = readUint64(r); err != nil {
		return err
	}
	req.nid, err = readUint64(r)
	return err
}

func (req *identityReq) encode(w io.Writer) error {
	if err := req.req.encode(w); err != nil {
		return err
	}
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
	transfer     bool   // special flag to indicate leadership transfer
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
	if req.lastLogTerm, err = readUint64(r); err != nil {
		return err
	}
	req.transfer, err = readBool(r)
	return err
}

func (req *voteReq) encode(w io.Writer) error {
	if err := req.req.encode(w); err != nil {
		return err
	}
	if err := writeUint64(w, req.lastLogIndex); err != nil {
		return err
	}
	if err := writeUint64(w, req.lastLogTerm); err != nil {
		return err
	}
	return writeBool(w, req.transfer)
}

// ------------------------------------------------------

type voteResp struct {
	resp
}

// ------------------------------------------------------

type appendReq struct {
	req
	prevLogIndex   uint64
	prevLogTerm    uint64
	ldrCommitIndex uint64
	numEntries     uint64
}

func (req *appendReq) rpcType() rpcType { return rpcAppendEntries }

func (req *appendReq) decode(r io.Reader) error {
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

func (req *appendReq) encode(w io.Writer) error {
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

type appendResp struct {
	resp
	lastLogIndex uint64
}

func (resp *appendResp) decode(r io.Reader) error {
	var err error
	if err = resp.resp.decode(r); err != nil {
		return err
	}
	resp.lastLogIndex, err = readUint64(r)
	return err
}

func (resp *appendResp) encode(w io.Writer) error {
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

	size, err := readUint64(r)
	if err != nil {
		return err
	}
	req.size = int64(size)
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
