package raft

import (
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
	if r.trace.received != nil {
		r.trace.received(r.id, rpc.req.from(), r.state, r.term, rpc.req)
	}

	result, err := rpcResult(0), error(nil)
	if rpc.req.getTerm() < r.term {
		result = staleTerm
	} else {
		if rpc.req.getTerm() > r.term {
			r.setState(Follower)
			r.setTerm(rpc.req.getTerm())
		}

		switch rpc.req.rpcType() {
		case rpcAppendEntries, rpcInstallSnap:
			r.setState(Follower)
			r.setLeader(rpc.req.from())
			resetTimer = true
		}

		// do actual processing of request
		switch req := rpc.req.(type) {
		case *voteReq:
			result = r.onVoteRequest(req)
			resetTimer = result == success
		case *appendEntriesReq:
			result, err = r.onAppendEntriesRequest(req)
			if result == unexpectedErr {
				r.appendErr = err
			}
		case *installSnapReq:
			result, err = r.onInstallSnapRequest(req)
		case *timeoutNowReq:
			r.onTimeoutNowRequest()
		default:
			fatal("raft.replyRPC(%T)", req)
		}
	}

	// construct response
	rpc.resp = rpc.req.rpcType().createResp(r, result)

	// if possible, drain any partially read requests
	if result == readErr {
		rpc.readErr = err
	} else {
		switch req := rpc.req.(type) {
		case *installSnapReq:
			if req.size > 0 {
				_, rpc.readErr = io.CopyN(ioutil.Discard, req.snapshot, req.size)
			}
		}
	}

	if result == unexpectedErr && r.trace.Error != nil {
		r.trace.Error(err)
	}
	if r.trace.sending != nil {
		r.trace.sending(r.id, rpc.req.from(), r.state, rpc.resp)
	}
	close(rpc.done)
	return resetTimer
}

func (r *Raft) onVoteRequest(req *voteReq) rpcResult {
	// if we already voted
	if r.votedFor != 0 {
		if r.votedFor == req.candidate { // same candidate we votedFor
			return success
		}
		return alreadyVoted
	}

	// reject if candidate’s log is not at least as up-to-date as ours
	if r.lastLogTerm > req.lastLogTerm || (r.lastLogTerm == req.lastLogTerm && r.lastLogIndex > req.lastLogIndex) {
		return logNotUptodate
	}

	r.setVotedFor(req.candidate)
	return success
}

func (r *Raft) onAppendEntriesRequest(req *appendEntriesReq) (rpcResult, error) {
	// reply false if log at req.prevLogIndex does not match
	if req.prevLogIndex > r.snapIndex {
		if req.prevLogIndex > r.lastLogIndex {
			return prevEntryNotFound, nil
		}

		var prevLogTerm uint64
		var prevEntry *entry
		if req.prevLogIndex == r.lastLogIndex {
			prevLogTerm = r.lastLogTerm
		} else {
			var err error
			prevEntry = &entry{}
			if err = r.storage.getEntry(req.prevLogIndex, prevEntry); err != nil {
				assert(err != errNoEntryFound, "unexpected error: %v", err)
				return unexpectedErr, err
			}
			prevLogTerm = prevEntry.term
			// we never get ErrnotFound here, because we are the goroutine who is compacting
		}
		if req.prevLogTerm != prevLogTerm {
			return prevTermMismatch, nil
		}

		// valid req: can we commit req.prevLogIndex ?
		if r.canCommit(req, req.prevLogIndex, req.prevLogTerm) {
			r.setCommitIndex(req.prevLogIndex)
			if err := r.applyCommitted(prevEntry); err != nil {
				assert(err != errNoEntryFound, "unexpected error: %v", err)
				return unexpectedErr, err
			}
		}
	}

	// valid req: let us consume entries
	index, term := req.prevLogIndex, req.prevLogTerm
	for _, ne := range req.entries {
		prevTerm := term
		index, term = ne.index, ne.term
		if ne.index <= r.snapIndex {
			continue
		}
		if ne.index <= r.lastLogIndex {
			me := &entry{}
			err := r.storage.getEntry(ne.index, me)
			assert(err != errNoEntryFound, "unexpected error: %v", err)
			if err != nil {
				return unexpectedErr, err
			}
			if me.term == ne.term {
				continue
			}

			// new entry conflicts with our entry
			// delete it and all that follow it
			debug(r, "log.deleteGTE", ne.index)
			err = r.storage.deleteGTE(ne.index, prevTerm)
			if err != nil {
				return unexpectedErr, err
			}
			if ne.index <= r.configs.Latest.Index {
				r.revertConfig()
			}
		}
		// new entry not in the log, append it
		if err := r.storage.appendEntry(ne); err != nil {
			return unexpectedErr, err
		}
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
			if err := r.applyCommitted(ne); err != nil {
				assert(err != errNoEntryFound, "unexpected error: %v", err)
				return unexpectedErr, err
			}
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
func (r *Raft) applyCommitted(ne *entry) error {
	for r.lastApplied < r.commitIndex {
		// get lastApplied+1 entry
		var e *entry
		if ne != nil && ne.index == r.lastApplied+1 {
			e = ne
		} else {
			e = &entry{}
			if err := r.storage.getEntry(r.lastApplied+1, e); err != nil {
				return err
			}
		}

		r.applyEntry(newEntry{entry: e})
		r.lastApplied++
		debug(r, "lastApplied", r.lastApplied)
	}
	return nil
}

func (r *Raft) onInstallSnapRequest(req *installSnapReq) (rpcResult, error) {
	// store snapshot
	sink, err := r.snapshots.New(req.lastIndex, req.lastTerm, req.lastConfig)
	if err != nil {
		return unexpectedErr, err
	}
	n, err := io.CopyN(sink, req.snapshot, req.size)
	req.size -= n
	if err != nil {
		_, _ = sink.Done(err)
		return readErr, err
	}
	meta, err := sink.Done(nil)
	if err != nil {
		return unexpectedErr, err
	}

	discardLog := true
	metaIndexExists := meta.Index > r.snapIndex && meta.Index <= r.lastLogIndex
	if metaIndexExists {
		metaTerm, err := r.storage.getEntryTerm(meta.Index)
		if err != nil {
			return unexpectedErr, err
		}
		termsMatched := metaTerm == meta.Term
		if termsMatched {
			// delete <=meta.index, but retain following it
			if err = r.storage.deleteLTE(meta); err != nil {
				return unexpectedErr, err
			}
			discardLog = false
		}
	}
	if discardLog {
		count := r.lastLogIndex - r.snapIndex
		if err = r.storage.log.DeleteFirst(count); err != nil {
			return unexpectedErr, err
		}
		r.lastLogIndex, r.lastLogTerm = meta.Index, meta.Term
		r.snapIndex, r.snapTerm = meta.Index, meta.Term

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

func (r *Raft) onTimeoutNowRequest() {
	r.setState(Candidate)
	r.setLeader(0)
}
