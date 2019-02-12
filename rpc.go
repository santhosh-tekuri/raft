package raft

import (
	"time"
)

func (r *Raft) processRPC(rpc rpc) {
	r.checkTerm(rpc.req)

	var resp command
	switch req := rpc.req.(type) {
	case *requestVoteRequest:
		resp = r.requestVote(req)
	case *appendEntriesRequest:
		resp = r.appendEntries(req)
	default:
		// todo
	}
	rpc.respCh <- resp
}

func (r *Raft) requestVote(req *requestVoteRequest) *requestVoteResponse {
	resp := &requestVoteResponse{
		term:        r.term,
		voteGranted: false,
	}

	// reply false if term < currentTerm
	if req.term < r.term {
		return resp
	}

	// reply false if we already voted
	if r.votedFor != "" {
		// reply true if votedFor is candidateId
		if r.votedFor == req.candidateID {
			resp.voteGranted = true
		}
		return resp
	}

	// reject if candidate’s log is not at
	// least as up-to-date as receiver’s log
	switch {
	case r.lastLogTerm > req.lastLogTerm:
	case r.lastLogTerm == req.lastLogTerm && r.lastLogIndex > req.lastLogIndex:
		return resp
	}

	debug(r, "grantVoteTo", req.candidateID)
	r.setVotedFor(req.candidateID)
	resp.voteGranted = true
	r.lastContact = time.Now()
	r.electionTimer.Stop()
	return resp
}

func (r *Raft) appendEntries(req *appendEntriesRequest) *appendEntriesResponse {
	resp := &appendEntriesResponse{
		term:         r.term,
		success:      false,
		lastLogIndex: r.lastLogIndex,
	}

	// reply false if term < currentTerm
	if req.term < r.term {
		debug(r, "older term")
		return resp
	}

	r.leaderID = req.leaderID

	// reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if req.prevLogIndex > 0 {
		var prevLogTerm uint64
		if req.prevLogIndex == r.lastLogIndex {
			prevLogTerm = r.lastLogTerm
		} else if req.prevLogIndex > r.lastLogIndex {
			// log doesn't contain an entry at prevLogIndex
			return resp
		} else {
			prevEntry := &entry{}
			r.storage.getEntry(req.prevLogIndex, prevEntry)
			prevLogTerm = prevEntry.term
			if req.prevLogTerm != prevLogTerm {
				// term did not match
				return resp
			}
		}
	}

	// valid request: stop election timer
	r.lastContact = time.Now()
	r.electionTimer.Stop()

	if len(req.entries) > 0 { // todo: should we check this. what if leader wants us to truncate
		// if an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it
		var newEntries []*entry
		for i, e := range req.entries {
			if e.index > r.lastLogIndex {
				newEntries = req.entries[i:]
				break
			}
			se := &entry{} // store entry
			r.storage.getEntry(e.index, se)
			if se.term != e.term {
				r.storage.deleteGTE(e.index)
				newEntries = req.entries[i:]
				break
			}
		}

		// append any new entries not already in the log
		debug(r, "log.appendN", len(newEntries))
		for _, e := range newEntries {
			r.storage.append(e)
		}
		if n := len(newEntries); n > 0 {
			last := newEntries[n-1]
			r.lastLogIndex, r.lastLogTerm = last.index, last.term
		}

		resp.lastLogIndex = r.lastLogIndex
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	// note: req.leaderCommitIndex==0 for heatbeat requests
	lastIndex, lastTerm := r.lastLog(req)
	if lastTerm == req.term && req.leaderCommitIndex > r.commitIndex {
		r.commitIndex = min(req.leaderCommitIndex, lastIndex)
		r.fsmApply(nil)
	}

	resp.success = true
	r.lastContact = time.Now()
	return resp
}

// if RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower
func (r *Raft) checkTerm(cmd command) {
	if cmd.getTerm() > r.term {
		debug(r, "staleTerm")
		r.setTerm(cmd.getTerm())
		if r.state != follower {
			debug(r, r.state, "-> follower")
		}
		r.state = follower
		r.leaderID = ""
		stateChanged(r)
	}
}

func (r *Raft) lastLog(req *appendEntriesRequest) (index uint64, term uint64) {
	switch n := len(req.entries); {
	case n == 0:
		return req.prevLogIndex, req.prevLogTerm
	default:
		last := req.entries[n-1]
		return last.index, last.term
	}
}
