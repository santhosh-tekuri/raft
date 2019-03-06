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
		r.stateChanged()
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
		r.stateChanged()
	}

	r.leader = req.leader

	// reply false if log at req.prevLogIndex does not match
	if req.prevLogIndex > 0 {
		if req.prevLogIndex > r.lastLogIndex {
			// no entry found
			debug(r, "received", req)
			debug(r, "rejectAppendEntries", req.prevLogIndex, "entryNotFound, i have", r.lastLogIndex)
			return resp
		}

		var prevLogTerm uint64
		if req.prevLogIndex == r.lastLogIndex {
			prevLogTerm = r.lastLogTerm
		} else if req.prevLogIndex == r.snapIndex {
			prevLogTerm = r.snapTerm
		} else {
			var err error
			prevLogTerm, err = r.storage.getEntryTerm(req.prevLogIndex)
			// we never get ErrnotFound here, because we are the goroutine who is compacting
			assert(err == nil, "unexpected error: %v", err)
		}
		if req.prevLogTerm != prevLogTerm {
			// term did not match
			debug(r, "received", req)
			debug(r, "rejectAppendEntries", "term mismatch", req.prevLogTerm, prevLogTerm)
			return resp
		}
	}

	// we came here, it means we got valid request
	if len(req.entries) > 0 {
		var newEntries []*entry
		var prevLogTerm = req.prevLogTerm
		// if new entry conflicts, delete it and all that follow it
		for i, ne := range req.entries {
			if ne.index > r.lastLogIndex {
				newEntries = req.entries[i:]
				break
			}
			e := &entry{}
			_ = r.storage.getEntry(ne.index, e)
			if e.term != ne.term { // conflicts
				debug(r, "log.deleteGTE", ne.index)
				r.storage.deleteGTE(ne.index, prevLogTerm)
				if ne.index <= r.configs.Latest.Index {
					r.revertConfig()
				}
				newEntries = req.entries[i:]
				break
			}
			prevLogTerm = ne.term
		}

		// append new entries not already in the log
		if len(newEntries) > 0 {
			debug(r, "received", req)
			debug(r, "log.appendN", "from:", newEntries[0].index, "n:", len(newEntries))
			for _, e := range newEntries {
				r.storage.appendEntry(e)
				if e.typ == entryConfig {
					var newConfig Config
					if err := newConfig.decode(e); err != nil {
						panic(err)
					}
					r.changeConfig(newConfig)
				}
			}
		}

		resp.lastLogIndex = r.lastLogIndex
	} else {
		debug(r, "received heartbeat")
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	lastIndex, lastTerm := lastEntry(req)
	if lastTerm == req.term && req.ldrCommitIndex > r.commitIndex {
		r.commitIndex = min(req.ldrCommitIndex, lastIndex)
		r.applyCommitted(nil) // apply newly committed logs
	}

	resp.success = true
	return resp
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
		r.stateChanged()
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
		restoreReq := fsmRestoreReq{task: newTask()}
		r.fsmTaskCh <- restoreReq
		<-restoreReq.Done()
		if restoreReq.Err() != nil {
			err = restoreReq.Err()
			debug(r, "fsmRestore failed:", err)
			return
		}

		// load snapshot config as cluster configuration
		r.changeConfig(meta.Config)
		r.commitConfig()
	}

	resp.success = true
	return
}
