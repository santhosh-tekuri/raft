package raft

import (
	"io"
)

type rpcType int

const (
	rpcRequestVote rpcType = iota
	rpcAppendEntries
)

type command interface {
	getTerm() uint64
	decode(r io.Reader) error
	encode(w io.Writer) error
}

type entry struct {
	index uint64
	term  uint64
	typ   uint8
	data  []byte
}

// implemented for testing purpose only
func (e *entry) getTerm() uint64 {
	panic("entry.getTerm() should not be called")
}

func (e *entry) decode(r io.Reader) error {
	var err error

	if e.index, err = readUint64(r); err != nil {
		return err
	}
	if e.term, err = readUint64(r); err != nil {
		return err
	}
	if e.typ, err = readUint8(r); err != nil {
		return err
	}
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
	if err := writeUint8(w, e.typ); err != nil {
		return err
	}
	if err := writeBytes(w, e.data); err != nil {
		return err
	}
	return nil
}

type requestVoteRequest struct {
	term         uint64 // candidate's term
	candidateID  string // candidate requesting vote
	lastLogIndex uint64 // index of candidate's last log entry
	lastLogTerm  uint64 // term of candidate's last log entry
}

func (req *requestVoteRequest) getTerm() uint64 {
	return req.term
}

func (req *requestVoteRequest) decode(r io.Reader) error {
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

func (req *requestVoteRequest) encode(w io.Writer) error {
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

type requestVoteResponse struct {
	term        uint64 // currentTerm, for candidate to update itself
	voteGranted bool   // true means candidate received vote
}

func (resp *requestVoteResponse) getTerm() uint64 {
	return resp.term
}

func (resp *requestVoteResponse) decode(r io.Reader) error {
	var err error

	if resp.term, err = readUint64(r); err != nil {
		return err
	}
	if resp.voteGranted, err = readBool(r); err != nil {
		return err
	}
	return nil
}

func (resp *requestVoteResponse) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeBool(w, resp.voteGranted); err != nil {
		return err
	}
	return nil
}

type appendEntriesRequest struct {
	term              uint64
	leaderID          string
	prevLogIndex      uint64
	prevLogTerm       uint64
	entries           []*entry
	leaderCommitIndex uint64
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

	if req.leaderCommitIndex, err = readUint64(r); err != nil {
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

	if err := writeUint64(w, req.leaderCommitIndex); err != nil {
		return err
	}
	return nil
}

type appendEntriesResponse struct {
	term    uint64
	success bool
	// todo: what abt additional fields NoRetryBackoff, LastLog
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
	return nil
}

func (resp *appendEntriesResponse) encode(w io.Writer) error {
	if err := writeUint64(w, resp.term); err != nil {
		return err
	}
	if err := writeBool(w, resp.success); err != nil {
		return err
	}
	return nil
}
