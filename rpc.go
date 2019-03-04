package raft

import (
	"fmt"
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
	var resetElectionTimer = true
	switch req := rpc.req.(type) {
	case *voteReq:
		reply := r.onVoteRequest(req)
		rpc.resp, resetElectionTimer = reply, reply.granted
	case *appendEntriesReq:
		rpc.resp = r.onAppendEntriesRequest(req)
	case *installSnapReq:
		rpc.resp, rpc.readErr = r.onInstallSnapRequest(req)
	default:
		// todo
	}
	close(rpc.done)
	return resetElectionTimer
}

func (r *Raft) onVoteRequest(req *voteReq) *voteResp {
	debug(r, "onVoteRequest", req.term, req.candidate, req.lastLogIndex, req.lastLogTerm)
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
			// no log found
			return resp
		}

		var prevLogTerm uint64
		if req.prevLogIndex == r.lastLogIndex {
			prevLogTerm = r.lastLogTerm
		} else if req.prevLogIndex == r.snapIndex {
			prevLogTerm = r.snapTerm
		} else {
			prevLogTerm = r.storage.getEntryTerm(req.prevLogIndex)
		}
		if req.prevLogTerm != prevLogTerm {
			// term did not match
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
			r.storage.getEntry(ne.index, e)
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
	debug(r, "onInstallSnapRequest", req.term, req.lastIndex, req.lastTerm, req.lastConfig)

	defer func() {
		_, err := io.Copy(ioutil.Discard, req.snapshot)
		if err != nil {
			readErr = err
		}
	}()
	resp = &installSnapResp{term: r.term, success: false}

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
		_, _ = sink.Done(readErr)
		return
	}
	if n != req.size {
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

	// ask fsm to restore from this snapshot
	restoreReq := fsmRestoreReq{task: newTask(), index: req.lastIndex}
	r.fsmTaskCh <- restoreReq
	<-restoreReq.Done()
	if restoreReq.Err() != nil {
		err = restoreReq.Err()
		return
	}

	r.snapIndex, r.snapTerm = meta.Index, meta.Term
	r.changeConfig(meta.Config)
	r.commitConfig()
	if err := r.storage.deleteLTE(meta.Index); err != nil {
		// send to trace
		fmt.Println("compactLogs failed:", err)
	}

	resp.success = true
	return
}
