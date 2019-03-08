package raft

import (
	"io"
	"io/ioutil"
)

// If election timeout elapses without receiving AppendEntries
// RPC from current leader or granting vote to candidate:
// convert to candidate.
//
// replyRPC return true if it is appendEntries request
// or vote granted
func (r *Raft) replyRPC(rpc *rpc) bool {
	if r.trace.received != nil {
		r.trace.received(r.id, rpc.req.from(), rpc.req)
	}
	var resetTimer = true
	switch req := rpc.req.(type) {
	case *voteReq:
		reply := r.onVoteRequest(req)
		rpc.resp, resetTimer = reply, reply.granted
	case *appendEntriesReq:
		rpc.resp = r.onAppendEntriesRequest(req)
	case *installSnapReq:
		rpc.resp, rpc.readErr = r.onInstallSnapRequest(req)
	default:
		// todo
	}
	close(rpc.done)
	if r.trace.sending != nil {
		r.trace.sending(r.id, rpc.req.from(), rpc.resp)
	}
	return resetTimer
}

func (r *Raft) onVoteRequest(req *voteReq) *voteResp {
	debug(r, "received", req)
	resp := &voteResp{
		term:    r.term,
		granted: false,
	}

	switch {
	case req.term < r.term: // reject: older term
		debug(r, "rejectVoteTo", req.candidate, "oldTerm")
		return resp
	case req.term > r.term: // convert to follower
		debug(r, "stateChange", req.term, Follower)
		r.state = Follower
		r.setTerm(req.term)
	}

	// if we have leader, we only vote for him
	if r.leader != "" {
		resp.granted = req.candidate == r.leader
		return resp
	}

	if r.votedFor != "" { // we already voted in this election before
		if r.votedFor == req.candidate { // same candidate we votedFor
			resp.granted = true
			debug(r, "grantVoteTo", req.candidate)
		} else {
			debug(r, "rejectVoteTo", req.candidate, "alreadyVotedTo", r.votedFor)
		}
		return resp
	}

	// reject if candidate’s log is not at least as up-to-date as ours
	if r.lastLogTerm > req.lastLogTerm || (r.lastLogTerm == req.lastLogTerm && r.lastLogIndex > req.lastLogIndex) {
		debug(r, "rejectVoteTo", req.candidate, "logNotUptoDate", r.lastLogIndex, r.lastLogTerm, req.lastLogIndex, req.lastLogTerm)
		return resp
	}

	debug(r, "grantVoteTo", req.candidate)
	resp.granted = true
	r.setVotedFor(req.candidate)

	return resp
}

func (r *Raft) onAppendEntriesRequest(req *appendEntriesReq) *appendEntriesResp {
	resp := &appendEntriesResp{
		term:         r.term,
		success:      false,
		lastLogIndex: r.lastLogIndex,
	}

	// reply false if older term
	if req.term < r.term {
		debug(r, "received", req)
		debug(r, "rejectAppendEntries", "stale term")
		return resp
	}

	// if newer term, convert to follower
	if req.term > r.term || r.state != Follower {
		debug(r, "stateChange", req.term, Follower)
		r.state = Follower
		r.setTerm(req.term)
	}

	r.leader = req.leader

	// reply false if log at req.prevLogIndex does not match
	if req.prevLogIndex > r.snapIndex {
		if req.prevLogIndex > r.lastLogIndex {
			// no entry found
			debug(r, "received", req)
			debug(r, "rejectAppendEntries", req.prevLogIndex, "entryNotFound, i have", r.lastLogIndex)
			return resp
		}

		var prevLogTerm uint64
		var prevEntry *entry
		if req.prevLogIndex == r.lastLogIndex {
			prevLogTerm = r.lastLogTerm
		} else {
			var err error
			prevEntry = &entry{}
			err = r.storage.getEntry(req.prevLogIndex, prevEntry)
			prevLogTerm = prevEntry.term
			// we never get ErrnotFound here, because we are the goroutine who is compacting
			assert(err == nil, "unexpected error: %v", err)
		}
		if req.prevLogTerm != prevLogTerm {
			// term did not match
			debug(r, "received", req)
			debug(r, "rejectAppendEntries", "term mismatch", req.prevLogTerm, prevLogTerm)
			return resp
		}

		// valid req: can we commit req.prevLogIndex ?
		if r.canCommit(req, req.prevLogIndex, req.prevLogTerm) {
			r.setCommitIndex(req.prevLogIndex)
			r.applyCommitted(prevEntry)
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
			_ = r.storage.getEntry(ne.index, me)
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
		r.storage.appendEntry(ne)
		if ne.typ == entryConfig {
			var newConfig Config
			if err := newConfig.decode(ne); err != nil {
				panic(err)
			}
			r.changeConfig(newConfig)
		}

		if r.canCommit(req, ne.index, ne.term) {
			r.setCommitIndex(ne.index)
			r.applyCommitted(ne)
		}
	}
	resp.lastLogIndex = r.lastLogIndex

	// if no entries
	if index == req.prevLogIndex {
		debug(r, "received heartbeat")
	}

	resp.success = true
	return resp
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
			r.storage.getEntry(r.lastApplied+1, e)
		}

		r.applyEntry(NewEntry{entry: e})
		r.lastApplied++
		debug(r, "lastApplied", r.lastApplied)
	}
}

func (r *Raft) canCommit(req *appendEntriesReq, index, term uint64) bool {
	return req.ldrCommitIndex >= index && // did leader committed it ?
		term == req.term && // don't commit any entry, until leader has committed an entry with his term
		index > r.commitIndex // haven't we committed yet
}

func lastEntry(req *appendEntriesReq) (index, term uint64) {
	if n := len(req.entries); n == 0 {
		return req.prevLogIndex, req.prevLogTerm
	} else {
		last := req.entries[n-1]
		return last.index, last.term
	}
}

func (r *Raft) onInstallSnapRequest(req *installSnapReq) (resp *installSnapResp, readErr error) {
	debug(r, "received", req)

	resp = &installSnapResp{term: r.term, success: false}
	defer func() {
		_, err := io.Copy(ioutil.Discard, req.snapshot)
		if err != nil {
			readErr = err
		}
	}()

	// reply false if older term
	if req.term < r.term {
		return
	}

	// if newer term, convert to follower
	if req.term > r.term || r.state != Follower {
		debug(r, "stateChange", req.term, Follower)
		r.state = Follower
		r.setTerm(req.term)
	}

	r.leader = req.leader

	// store snapshot
	sink, err := r.snapshots.New(req.lastIndex, req.lastTerm, req.lastConfig)
	if err != nil {
		debug(r, "snapshots.New failed", err)
		// todo: send to trace
		return
	}
	n, readErr := io.Copy(sink, req.snapshot)
	if readErr != nil {
		debug(r, "copy snapshot->sink failed:", readErr)
		_, _ = sink.Done(readErr)
		return
	}
	if n != req.size {
		debug(r, "installSnapReq size mismatch", n, req.size)
		readErr = io.ErrUnexpectedEOF
		_, _ = sink.Done(readErr)
		return
	}
	meta, err := sink.Done(nil)
	if err != nil {
		debug(r, "sing.Done failed", err)
		// todo: send to trace
		return
	}

	discardLog := true
	metaIndexExists := meta.Index > r.snapIndex && meta.Index <= r.lastLogIndex
	if metaIndexExists {
		metaTerm, err := r.storage.getEntryTerm(meta.Index)
		if err != nil {
			panic(err)
		}
		termsMatched := metaTerm == meta.Term
		if termsMatched {
			// delete <=meta.index, but retain following it
			if err = r.storage.deleteLTE(meta); err != nil {
				assert(false, err.Error())
			}
			discardLog = false
		}
	}
	if discardLog {
		count := r.lastLogIndex - r.snapIndex
		if err = r.storage.log.DeleteFirst(count); err != nil {
			assert(false, err.Error())
		}
		r.lastLogIndex, r.lastLogTerm = meta.Index, meta.Term
		r.snapIndex, r.snapTerm = meta.Index, meta.Term

		// reset fsm from this snapshot
		if err = r.restoreFSMFromSnapshot(); err != nil {
			return
		}
		// load snapshot config as cluster configuration
		r.changeConfig(meta.Config)
		r.commitConfig()
	}

	resp.success = true
	return
}
