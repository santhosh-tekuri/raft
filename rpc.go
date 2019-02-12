package raft

func (r *Raft) replyRPC(rpc rpc) {
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
	debug(r, "-> requestVoteFor", req.term, req.candidateID)
	resp := &requestVoteResponse{
		term:        r.term,
		voteGranted: false,
	}

	switch {
	case req.term < r.term: // reject: older term
		debug(r, "rejectVoteTo", req.candidateID, "oldTerm")
		return resp
	case req.term > r.term: // conver to follower
		debug(r, "stateChange", req.term, follower)
		r.state = follower
		r.setTerm(req.term)
		stateChanged(r)
	}

	if r.votedFor != "" { // we already voted in this election before
		if r.votedFor == req.candidateID { // same candidate we votedFor
			resp.voteGranted = true
			debug(r, "grantVoteTo", req.candidateID)
		} else {
			debug(r, "rejectVoteTo", req.candidateID, "alreadyVotedTo", r.votedFor)
		}
		return resp
	}

	// reject if candidate’s log is not at least as up-to-date as ours
	if r.lastLogTerm > req.lastLogTerm || (r.lastLogTerm == req.lastLogTerm && r.lastLogIndex > req.lastLogIndex) {
		debug(r, "rejectVoteTo", req.candidateID, "logNotUptoDate")
		return resp
	}

	debug(r, "grantVoteTo", req.candidateID)
	resp.voteGranted = true
	r.setVotedFor(req.candidateID)
	r.electionTimer.Stop()

	return resp
}

func (r *Raft) appendEntries(req *appendEntriesRequest) *appendEntriesResponse {
	resp := &appendEntriesResponse{
		term:         r.term,
		success:      false,
		lastLogIndex: r.lastLogIndex,
	}

	// reply false if older term
	if req.term < r.term {
		return resp
	}

	// if newer term, convert to follower
	if req.term > r.term || r.state != follower {
		debug(r, "stateChange", req.term, follower)
		r.state = follower
		r.setTerm(req.term)
		stateChanged(r)
	}

	r.leaderID = req.leaderID

	// reply false if log at req.prevLogIndex does not match
	if req.prevLogIndex > 0 {
		if req.prevLogIndex > r.lastLogIndex {
			// no log found
			return resp
		}

		var prevLogTerm uint64
		if req.prevLogIndex == r.lastLogIndex {
			prevLogTerm = r.lastLogTerm
		} else {
			prevEntry := &entry{}
			r.storage.getEntry(req.prevLogIndex, prevEntry)
			prevLogTerm = prevEntry.term
		}

		if req.prevLogTerm != prevLogTerm {
			// term did not match
			return resp
		}
	}

	// we came here, it means we got valid request

	r.electionTimer.Stop()
	if len(req.entries) > 0 { // todo: should we check this. what if leader wants us to truncate
		var newEntries []*entry

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
				r.storage.deleteGTE(ne.index)
				newEntries = req.entries[i:]
				break
			}
		}

		// append new entries not already in the log
		if len(newEntries) > 0 {
			debug(r, "log.appendN", "from:", newEntries[0].index, "n:", len(newEntries))
			for _, e := range newEntries {
				r.storage.append(e)
			}
			last := newEntries[len(newEntries)-1]
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
		r.fsmApply(nil) // apply newly committed logs
	}

	resp.success = true
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
