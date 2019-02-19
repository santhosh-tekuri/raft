package raft

import "time"

func (r *Raft) runCandidate() {
	timeoutCh := afterRandomTimeout(r.heartbeatTimeout)
	results := r.startElection()
	votesNeeded := r.quorumSize()
	for r.state == candidate {
		select {
		case <-r.shutdownCh:
			return

		case rpc := <-r.server.rpcCh:
			r.replyRPC(rpc)

		case vote := <-results:
			if vote.voterID != r.addr {
				debug(r, "<< voteResponse", vote.voterID, vote.granted, vote.term)
			}

			if r.checkTerm(vote); r.state != candidate {
				return
			}

			// if votes received from majority of servers: become leader
			if vote.granted {
				votesNeeded--
				if votesNeeded <= 0 {
					debug(r, "candidate -> leader")
					r.state = leader
					stateChanged(r)
					r.leaderID = r.addr
					return
				}
			}
		case <-timeoutCh:
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
			term:    r.term,
			granted: true,
		},
		voterID: r.addr,
	}

	// reset election timer
	debug(r, "startElection", time.Now().UTC())

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
					term:    req.term,
					granted: false,
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
