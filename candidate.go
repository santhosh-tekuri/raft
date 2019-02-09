package raft

import "time"

func (r *Raft) runCandidate() {
	results := r.startElection()
	votesNeeded := r.quorumSize()
	for r.state == candidate {
		select {
		case rpc := <-r.server.rpcCh:
			// if AppendEntries RPC received from new leader: convert to follower
			if _, ok := rpc.req.(*appendEntriesRequest); ok {
				r.state = follower
			}

			r.processRPC(rpc)

		case vote := <-results:
			if r.checkTerm(vote); r.state != candidate {
				return
			}

			// if votes received from majority of servers: become leader
			if vote.voteGranted {
				votesNeeded--
				if vote.voterID != r.addr {
					debug(r, vote.voterID, "voted for me")
				}
				if votesNeeded <= 0 {
					r.electionTimer.Stop()
					debug(r, "candidate -> leader")
					r.state = leader
					stateChanged(r)
					r.leaderID = r.addr
					return
				}
			}
		case <-r.electionTimer.C:
			// election timeout elapsed: start new election
			return

		case newEntry := <-r.applyCh:
			newEntry.sendResponse(NotLeaderError{r.leaderID})
		case f := <-r.inspectCh:
			f(r)
		}
	}
}

type voteResult struct {
	*requestVoteResponse
	voterID string
}

func (r *Raft) startElection() <-chan voteResult {
	results := make(chan voteResult, len(r.members))

	// increment currentTerm
	r.setTerm(r.term + 1)

	// vote for self
	r.setVotedFor(r.addr)
	results <- voteResult{
		requestVoteResponse: &requestVoteResponse{
			term:        r.term,
			voteGranted: true,
		},
		voterID: r.addr,
	}

	// reset election timer
	r.electionTimer = time.NewTimer(randomTimeout(r.heartbeatTimeout))
	debug(r, "starting election")

	// send RequestVote RPCs to all other servers
	req := &requestVoteRequest{
		term:         r.term,
		candidateID:  r.addr,
		lastLogIndex: r.lastLogIndex,
		lastLogTerm:  r.lastLogTerm,
	}
	for _, m := range r.members {
		if m.addr == r.addr {
			continue
		}
		go func(m *member) {
			result := voteResult{
				requestVoteResponse: &requestVoteResponse{
					term:        req.term,
					voteGranted: false,
				},
				voterID: m.addr,
			}
			defer func() {
				results <- result
			}()
			resp, err := m.requestVote(req)
			if err != nil {
				return
			}
			result.requestVoteResponse = resp
		}(m)
	}
	return results
}
