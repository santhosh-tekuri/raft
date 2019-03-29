package raft

import (
	"fmt"
	"io"
	"io/ioutil"
)

// resetTimer tells whether follower should reset its electionTimer or not
//
// Diego says:
//   If election timeout elapses without receiving AppendEntries
//   RPC from current leader or granting vote to candidate:
//   convert to candidate.
func (r *Raft) replyRPC(rpc *rpc) (resetTimer bool) {
	// handle identity req
	if req, ok := rpc.req.(*identityReq); ok {
		if r.cid != req.cid || r.nid != req.nid {
			rpc.resp = rpcIdentity.createResp(r, idMismatch, nil)
		} else {
			rpc.resp = rpcIdentity.createResp(r, success, nil)
		}
		close(rpc.done)
		return
	}

	debug(r, "<<", rpc.req)
	result, err := r.onRequest(rpc.req, rpc.reader)
	rpc.resp = rpc.req.rpcType().createResp(r, result, err)
	debug(r, ">>", rpc.resp)
	if result == readErr {
		rpc.readErr = err
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

func (r *Raft) onRequest(req request, reader io.Reader) (result rpcResult, err error) {
	defer func() {
		if v := recover(); v != nil {
			result, err = unexpectedErr, toErr(v)
		}
	}()

	switch req := req.(type) {
	case *voteReq:
		return r.onVoteRequest(req)
	case *appendEntriesReq:
		return r.onAppendEntriesRequest(req, reader)
	case *installSnapReq:
		return r.onInstallSnapRequest(req, reader)
	case *timeoutNowReq:
		return r.onTimeoutNowRequest()
	default:
		panic(fmt.Errorf("[BUG] raft.onRequest(%T)", req))
	}
}

// onVoteRequest -------------------------------------------------

func (r *Raft) onVoteRequest(req *voteReq) (rpcResult, error) {
	if req.term < r.term {
		return staleTerm, nil
	} else if req.term > r.term {
		r.setTerm(req.getTerm())
		r.setState(Follower)
	}

	// if we already voted
	if r.votedFor != 0 {
		if r.votedFor == req.src { // same candidate we votedFor
			return success, nil
		}
		return alreadyVoted, nil
	}

	// reject if candidate’s log is not at least as up-to-date as ours
	if r.lastLogTerm > req.lastLogTerm || (r.lastLogTerm == req.lastLogTerm && r.lastLogIndex > req.lastLogIndex) {
		return logNotUptodate, nil
	}

	r.setVotedFor(req.src)
	return success, nil
}

// onAppendEntriesRequest -------------------------------------------------

func (r *Raft) onAppendEntriesRequest(req *appendEntriesReq, reader io.Reader) (rpcResult, error) {
	drain := func(result rpcResult, err error) (rpcResult, error) {
		for req.numEntries > 0 {
			req.numEntries--
			ne := &entry{}
			if err := ne.decode(reader); err != nil {
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
	index, term := req.prevLogIndex, req.prevLogTerm
	for req.numEntries > 0 {
		req.numEntries--
		ne := &entry{}
		if err := ne.decode(reader); err != nil {
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
			debug(r, "log.deleteGTE", ne.index)
			r.storage.deleteGTE(ne.index, prevTerm)
			if ne.index <= r.configs.Latest.Index {
				r.revertConfig()
			}
		}
		// new entry not in the log, append it
		debug(r, "log.append", ne.typ, ne.index)
		r.storage.appendEntry(ne)
		if ne.typ == entryConfig {
			var newConfig Config
			if err := newConfig.decode(ne); err != nil {
				return unexpectedErr, err
			}
			r.changeConfig(newConfig)
		}

		_ = index
		if r.canCommit(req, ne.index, ne.term) {
			r.setCommitIndex(ne.index)
			r.applyCommitted(ne)
		}
	}
	return success, nil
}

func (r *Raft) canCommit(req *appendEntriesReq, index, term uint64) bool {
	return req.ldrCommitIndex >= index && // did leader committed it ?
		term == req.term && // don't commit any entry, until leader has committed an entry with his term
		index > r.commitIndex // haven't we committed yet
}

// if commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
func (r *Raft) applyCommitted(ne *entry) {
	for r.lastApplied < r.commitIndex {
		// get lastApplied+1 entry
		var e *entry
		if ne != nil && ne.index == r.lastApplied+1 {
			e = ne
		} else {
			e = &entry{}
			r.storage.mustGetEntry(r.lastApplied+1, e)
		}

		r.applyEntry(newEntry{entry: e})
		r.lastApplied++
		debug(r, "lastApplied", r.lastApplied)
	}
}

// onInstallSnapRequest -------------------------------------------------

func (r *Raft) onInstallSnapRequest(req *installSnapReq, reader io.Reader) (rpcResult, error) {
	drain := func(result rpcResult, err error) (rpcResult, error) {
		if req.size > 0 {
			if _, errr := io.CopyN(ioutil.Discard, reader, req.size); errr != nil {
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
	n, err := io.CopyN(sink.file, reader, req.size)
	req.size -= n
	if err != nil {
		_, _ = sink.done(err)
		return readErr, err
	}
	meta, err := sink.done(nil)
	if err != nil {
		return unexpectedErr, opError(err, "snapshotSink.done")
	}

	discardLog := true
	metaIndexExists := meta.Index > r.prevLogIndex && meta.Index <= r.lastLogIndex
	if metaIndexExists {
		metaTerm, err := r.storage.getEntryTerm(meta.Index)
		if err != nil {
			return unexpectedErr, err
		}
		termsMatched := metaTerm == meta.Term
		if termsMatched {
			// delete <=meta.index, but retain following it
			if err = r.storage.deleteLTE(meta.Index); err != nil {
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

		// reset fsm from this snapshot
		if err = r.restoreFSM(); err != nil {
			return unexpectedErr, err
		}
		// load snapshot config as cluster configuration
		r.changeConfig(meta.Config)
		r.commitConfig()
	}

	return success, nil
}

// onTimeoutNowRequest -------------------------------------------------

func (r *Raft) onTimeoutNowRequest() (rpcResult, error) {
	r.setState(Candidate)
	r.setLeader(0)
	return success, nil
}
