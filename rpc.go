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
	"fmt"
	"io"
	"io/ioutil"
)

// resetTimer tells whether follower should reset its electionTimer or not
//
// from thesis:
//   If election timeout elapses without receiving AppendEntries
//   RPC from current leader or granting vote to candidate:
//   convert to candidate.
func (r *Raft) replyRPC(rpc *rpc) (resetTimer bool) {
	// handle identity req
	if req, ok := rpc.req.(*identityReq); ok {
		if r.cid != req.cid || r.nid != req.nid {
			rpc.resp = rpcIdentity.createResp(r, identityMismatch, nil)
		} else {
			rpc.resp = rpcIdentity.createResp(r, success, nil)
		}
		close(rpc.done)
		return req.src == r.leader
	}

	if trace {
		println(r, "<<", rpc.req)
	}
	result, err := r.onRequest(rpc.req, rpc.conn)
	rpc.resp = rpc.req.rpcType().createResp(r, result, err)
	if result == readErr {
		rpc.readErr = err
	}
	if trace {
		println(r, ">>", rpc.resp)
	}
	close(rpc.done)

	if result == unexpectedErr {
		if r.trace.Error != nil {
			r.trace.Error(err)
		}
		panic(err)
	}
	return rpc.req.rpcType() != rpcVote || result == success
}

func (r *Raft) onRequest(req request, c *conn) (result rpcResult, err error) {
	defer func() {
		if v := recover(); v != nil {
			result, err = unexpectedErr, toErr(v)
		}
	}()

	switch req := req.(type) {
	case *voteReq:
		return r.onVoteRequest(req)
	case *appendReq:
		return r.onAppendEntriesRequest(req, c)
	case *installSnapReq:
		return r.onInstallSnapRequest(req, c)
	case *timeoutNowReq:
		return r.onTimeoutNowRequest()
	default:
		panic(fmt.Errorf("[BUG] raft.onRequest(%T)", req))
	}
}

// onVoteRequest -------------------------------------------------

func (r *Raft) onVoteRequest(req *voteReq) (rpcResult, error) {
	// to avoid hitting disk at most once
	term, votedFor := r.term, r.votedFor
	defer func() {
		r.setVotedFor(term, votedFor)
	}()

	// 4.2.3: to solve the problem of disruptive servers:
	// if a server receives a RequestVote request within the minimum
	// election timeout of hearing from a current leader, it does not
	// update its term or grant its vote.
	//
	// RequestVote requests used for leadership transfer can include
	// a special flag to indicate this behavior:
	// "I have permission to disrupt the leader—it told me to!"
	if !req.transfer && r.leader != 0 {
		if req.src == r.leader {
			return success, nil
		}
		return leaderKnown, nil
	}

	if req.term < r.term {
		return staleTerm, nil
	} else if req.term > r.term {
		term, votedFor = req.getTerm(), 0
		r.setState(Follower)
	}

	// if we already voted
	if votedFor != 0 {
		if votedFor == req.src { // same candidate we votedFor
			return success, nil
		}
		return alreadyVoted, nil
	}

	// reject if candidate’s log is not at least as up-to-date as ours
	if r.lastLogTerm > req.lastLogTerm || (r.lastLogTerm == req.lastLogTerm && r.lastLogIndex > req.lastLogIndex) {
		return logNotUptodate, nil
	}

	votedFor = req.src
	return success, nil
}

// onAppendEntriesRequest -------------------------------------------------

func (r *Raft) onAppendEntriesRequest(req *appendReq, c *conn) (rpcResult, error) {
	drain := func(result rpcResult, err error) (rpcResult, error) {
		for req.numEntries > 0 {
			req.numEntries--
			ne := &entry{}
			if err := ne.decode(c.bufr); err != nil {
				return readErr, err
			}
		}
		return result, err
	}

	if req.term < r.term {
		return drain(staleTerm, nil)
	} else if req.term > r.term {
		r.setTerm(req.getTerm())
		r.setState(Follower)
	}
	r.setState(Follower)
	r.setLeader(req.src)

	// reply false if log at req.prevLogIndex does not match
	if req.prevLogIndex > r.snaps.index {
		if req.prevLogIndex > r.lastLogIndex {
			return drain(prevEntryNotFound, nil)
		}

		var prevLogTerm uint64
		var prevEntry *entry
		if req.prevLogIndex == r.lastLogIndex {
			prevLogTerm = r.lastLogTerm
		} else {
			prevEntry = &entry{}
			r.storage.mustGetEntry(req.prevLogIndex, prevEntry)
			prevLogTerm = prevEntry.term
		}
		if req.prevLogTerm != prevLogTerm {
			return drain(prevTermMismatch, nil)
		}

		// valid req: can we commit req.prevLogIndex ?
		if r.canCommit(req, req.prevLogIndex, req.prevLogTerm) {
			r.setCommitIndex(req.prevLogIndex)
			r.applyCommitted(prevEntry)
		}
	}

	// valid req: let us consume entries
	index, term, syncLog := req.prevLogIndex, req.prevLogTerm, false
	if req.numEntries > 0 {
		defer func() {
			if syncLog {
				if trace {
					println(r, "log.Commit", r.lastLogIndex)
				}
				r.storage.commitLog(r.lastLogIndex)
				if r.canCommit(req, index, term) {
					r.setCommitIndex(index)
					r.applyCommitted(nil)
				}
			}
		}()
	}
	for req.numEntries > 0 {
		req.numEntries--
		if !isEntryBuffered(c.bufr) {
			if err := c.rwc.SetReadDeadline(r.rtime.deadline(r.hbTimeout)); err != nil {
				return readErr, err
			}
		}
		ne := &entry{}
		if err := ne.decode(c.bufr); err != nil {
			return readErr, err
		}
		prevTerm := term
		index, term = ne.index, ne.term
		if ne.index <= r.snaps.index {
			continue
		}
		if ne.index <= r.lastLogIndex {
			me := &entry{}
			r.storage.mustGetEntry(ne.index, me)
			if me.term == ne.term {
				continue
			}

			// new entry conflicts with our entry
			// delete it and all that follow it
			if trace {
				println(r, "log.removeGTE", ne.index)
			}
			r.storage.removeGTE(ne.index, prevTerm)
			if ne.index <= r.configs.Latest.Index {
				r.revertConfig()
			}
		}
		// new entry not in the log, append it
		if trace {
			println(r, "log.append", ne.typ, ne.index)
		}
		r.storage.appendEntry(ne)
		syncLog = true
		if ne.typ == entryConfig {
			var newConfig Config
			if err := newConfig.decode(ne); err != nil {
				return unexpectedErr, err
			}
			r.changeConfig(newConfig)
		}
	}
	return success, nil
}

func (r *Raft) canCommit(req *appendReq, index, term uint64) bool {
	return req.ldrCommitIndex >= index && // did leader committed it ?
		term == req.term && // don't commit any entry, until leader has committed an entry with his term
		index > r.commitIndex // haven't we committed yet
}

// if commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
func (r *Raft) applyCommitted(ne *entry) {
	apply := fsmApply{log: r.log.ViewAt(r.log.PrevIndex(), r.commitIndex)}
	if trace {
		println(r, apply)
	}
	r.fsm.ch <- apply
}

// onInstallSnapRequest -------------------------------------------------

func (r *Raft) onInstallSnapRequest(req *installSnapReq, c *conn) (rpcResult, error) {
	drain := func(result rpcResult, err error) (rpcResult, error) {
		if req.size > 0 {
			if _, errr := io.CopyN(ioutil.Discard, c.bufr, req.size); errr != nil {
				return readErr, errr
			}
		}
		return result, err
	}
	if req.term < r.term {
		return drain(staleTerm, nil)
	} else if req.term > r.term {
		r.setTerm(req.getTerm())
		r.setState(Follower)
	}
	r.setState(Follower)
	r.setLeader(req.src)

	// store snapshot
	sink, err := r.snaps.new(req.lastIndex, req.lastTerm, req.lastConfig)
	if err != nil {
		return unexpectedErr, opError(err, "snapshots.new")
	}
	n, err := io.CopyN(sink.file, c.bufr, req.size)
	req.size -= n
	meta, doneErr := sink.done(err)
	if err != nil {
		return readErr, err
	}
	if doneErr != nil {
		return unexpectedErr, opError(doneErr, "snapshotSink.done")
	}

	discardLog := true
	if r.storage.log.Contains(meta.index) {
		metaTerm, err := r.storage.getEntryTerm(meta.index)
		if err != nil {
			return unexpectedErr, err
		}
		termsMatched := metaTerm == meta.term
		if termsMatched {
			// remove <=meta.index, but retain following it
			if err = r.compactLog(meta.index); err != nil {
				return unexpectedErr, err
			}
			discardLog = false
		}
	}
	if discardLog {
		if err = r.storage.clearLog(); err != nil {
			return unexpectedErr, err
		}

		// todo: dont wait for restoreFSM to complete
		//       if restoreFSM fails panic and exit
		//       if takeSnap req came meanwhile, reply inProgress(restoreFSM)

		// restore fsm from this snapshot
		r.fsm.ch <- fsmRestoreReq{r.fsmRestoredCh}
		r.commitIndex = r.snaps.index

		// load snapshot config as cluster configuration
		r.changeConfig(meta.config)
		r.commitConfig()
	}

	return success, nil
}

// onTimeoutNowRequest -------------------------------------------------

func (r *Raft) onTimeoutNowRequest() (rpcResult, error) {
	r.setState(Candidate)
	r.setLeader(0)
	r.cnd.transfer = true
	return success, nil
}
