package raft

// return if it is validate request
func (r *Raft) replyRPC(rpc rpc) bool {
	var resp message
	var valid bool
	switch req := rpc.req.(type) {
	case *voteRequest:
		reply := r.onVoteRequest(req)
		resp, valid = reply, reply.granted
	case *appendEntriesRequest:
		reply := r.onAppendEntriesRequest(req)
		resp, valid = reply, reply.success
	default:
		// todo
	}
	rpc.respCh <- resp
	return valid
}

func (r *Raft) onVoteRequest(req *voteRequest) *voteResponse {
	debug(r, "<< voteRequest", req.term, req.candidateID, req.lastLogIndex, req.lastLogTerm)
	gotVoteRequest(r, req)
	resp := &voteResponse{
		term:    r.term,
		granted: false,
	}

	switch {
	case req.term < r.term: // reject: older term
		debug(r, "rejectVoteTo", req.candidateID, "oldTerm")
		return resp
	case req.term > r.term: // convert to follower
		debug(r, "stateChange", req.term, follower)
		r.state = follower
		r.setTerm(req.term)
		StateChanged(r, byte(r.state))
	}

	if r.votedFor != "" { // we already voted in this election before
		if r.votedFor == req.candidateID { // same candidate we votedFor
			resp.granted = true
			debug(r, "grantVoteTo", req.candidateID)
		} else {
			debug(r, "rejectVoteTo", req.candidateID, "alreadyVotedTo", r.votedFor)
		}
		return resp
	}

	// reject if candidate’s log is not at least as up-to-date as ours
	if r.lastLogTerm > req.lastLogTerm || (r.lastLogTerm == req.lastLogTerm && r.lastLogIndex > req.lastLogIndex) {
		debug(r, "rejectVoteTo", req.candidateID, "logNotUptoDate", r.lastLogIndex, r.lastLogTerm, req.lastLogIndex, req.lastLogTerm)
		return resp
	}

	debug(r, "grantVoteTo", req.candidateID)
	resp.granted = true
	r.setVotedFor(req.candidateID)

	return resp
}

func (r *Raft) onAppendEntriesRequest(req *appendEntriesRequest) *appendEntriesResponse {
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
		StateChanged(r, byte(r.state))
	}

	r.leader = req.leaderID

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
				if ne.index <= r.configs.latest.index {
					r.configs.latest = r.configs.committed
					r.storage.setConfigs(r.configs)
				}
				newEntries = req.entries[i:]
				break
			}
		}

		// append new entries not already in the log
		if len(newEntries) > 0 {
			debug(r, "log.appendN", "from:", newEntries[0].index, "n:", len(newEntries))
			var confEntry *entry
			for _, e := range newEntries {
				r.storage.append(e)
				if e.typ == entryConfig {
					confEntry = e
					r.configs.committed = r.configs.latest
				}
			}
			if confEntry != nil {
				if err := r.configs.latest.decode(confEntry); err != nil {
					panic(err)
				}
				r.storage.setConfigs(r.configs)
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

func (r *Raft) lastLog(req *appendEntriesRequest) (index uint64, term uint64) {
	switch n := len(req.entries); {
	case n == 0:
		return req.prevLogIndex, req.prevLogTerm
	default:
		last := req.entries[n-1]
		return last.index, last.term
	}
}
