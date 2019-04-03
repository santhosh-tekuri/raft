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
	// to avoid hitting disk at most once
	term, votedFor := r.term, r.votedFor
	defer func() {
		r.setVotedFor(term, votedFor)
	}()

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
	index, term, syncLog := req.prevLogIndex, req.prevLogTerm, false
	if req.numEntries > 0 {
		defer func() {
			if syncLog {
				debug(r, "commitLog")
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
			debug(r, "log.removeGTE", ne.index)
			r.storage.removeGTE(ne.index, prevTerm)
			if ne.index <= r.configs.Latest.Index {
				r.revertConfig()
			}
		}
		// new entry not in the log, append it
		debug(r, "log.append", ne.typ, ne.index)
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

func (r *Raft) canCommit(req *appendEntriesReq, index, term uint64) bool {
	return req.ldrCommitIndex >= index && // did leader committed it ?
		term == req.term && // don't commit any entry, until leader has committed an entry with his term
		index > r.commitIndex // haven't we committed yet
}

// if commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
func (r *Raft) applyCommitted(ne *entry) {
	apply := fsmApply{log: r.log.ViewAt(r.log.PrevIndex(), r.commitIndex)}
	debug(r, apply)
	r.fsm.ch <- apply
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
	if r.storage.log.Contains(meta.index) {
		metaTerm, err := r.storage.getEntryTerm(meta.index)
		if err != nil {
			return unexpectedErr, err
		}
		termsMatched := metaTerm == meta.term
		if termsMatched {
			// remove <=meta.index, but retain following it
			if err = r.storage.removeLTE(meta.index); err != nil {
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
	return success, nil
}
