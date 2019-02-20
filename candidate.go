package raft

import "time"

func (r *Raft) runCandidate() {
	assert(r.leaderID == "", "%s r.leaderID: got %s, want ", r, r.leaderID)
	timeoutCh := afterRandomTimeout(r.heartbeatTimeout)
	results := r.startElection()
	votes := make(map[string]struct{})
	for r.state == candidate {
		select {
		case <-r.shutdownCh:
			return

		case rpc := <-r.rpcCh:
			r.replyRPC(rpc)

		case vote := <-results:
			if vote.voterID != r.addr {
				debug(r, "<< voteResponse", vote.voterID, vote.granted, vote.term, vote.err)
			}

			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			if vote.term > r.term {
				debug(r, "candidate -> follower")
				r.state = follower
				r.setTerm(vote.term)
				stateChanged(r)
				return
			}

			// if votes received from majority of servers: become leader
			if vote.granted {
				votes[vote.voterID] = struct{}{}
				if r.config.isQuorum(votes) {
					debug(r, "candidate -> leader")
					r.state = leader
					r.leaderID = r.addr
					stateChanged(r)
					return
				}
			}
		case <-timeoutCh:
			// election timeout elapsed: start new election
			return

		case ne := <-r.ApplyCh:
			ne.sendResponse(NotLeaderError{r.leaderID})

		case f := <-r.inspectCh:
			f(r)
		}
	}
}

type voteResult struct {
	*voteResponse
	voterID string
	err     error
}

func (r *Raft) startElection() <-chan voteResult {
	results := make(chan voteResult, len(r.config.members()))

	// increment currentTerm
	r.setTerm(r.term + 1)

	// reset election timer
	debug(r, "startElection", time.Now().UnixNano()/int64(time.Millisecond))

	// send RequestVote RPCs to all other servers
	req := &voteRequest{
		term:         r.term,
		candidateID:  r.addr,
		lastLogIndex: r.lastLogIndex,
		lastLogTerm:  r.lastLogTerm,
	}
	for _, m := range r.config.members() {
		if m.addr == r.addr {
			// vote for self
			r.setVotedFor(r.addr)
			results <- voteResult{
				voteResponse: &voteResponse{
					term:    r.term,
					granted: true,
				},
				voterID: r.addr,
			}
			continue
		}
		go func(m *member) {
			result := voteResult{
				voteResponse: &voteResponse{
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
				result.err = err
				return
			}
			result.voteResponse = resp
		}(m)
	}
	return results
}
