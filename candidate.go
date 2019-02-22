package raft

import "time"

func (r *Raft) runCandidate() {
	assert(r.leaderID == "", "%s r.leaderID: got %s, want ", r, r.leaderID)
	timeoutCh := afterRandomTimeout(r.heartbeatTimeout)
	results := r.startElection()
	votesNeeded := r.configs.latest.quorum()
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

			if vote.err != nil {
				continue
			}
			// if response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			if vote.term > r.term {
				debug(r, "candidate -> follower")
				r.state = follower
				r.setTerm(vote.term)
				StateChanged(r, byte(r.state))
				return
			}

			// if votes received from majority of servers: become leader
			if vote.granted {
				votesNeeded--
				if votesNeeded == 0 {
					debug(r, "candidate -> leader")
					r.state = leader
					r.leaderID = r.addr
					StateChanged(r, byte(r.state))
					return
				}
			}
		case <-timeoutCh:
			// election timeout elapsed: start new election
			return

		case t := <-r.TasksCh:
			t.execute(r)
		}
	}
}

type voteResult struct {
	*voteResponse
	voterID string
	err     error
}

func (r *Raft) startElection() <-chan voteResult {
	results := make(chan voteResult, len(r.configs.latest.nodes))

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
	for _, n := range r.configs.latest.nodes {
		if !n.voter {
			continue
		}
		if n.addr == r.addr {
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
		go func(n node) {
			result := voteResult{
				voteResponse: &voteResponse{
					term:    req.term,
					granted: false,
				},
				voterID: n.addr,
			}
			defer func() {
				results <- result
			}()
			resp, err := r.requestVote(n.addr, req)
			if err != nil {
				result.err = err
				return
			}
			result.voteResponse = resp
		}(n)
	}
	return results
}

func (r *Raft) requestVote(addr string, req *voteRequest) (*voteResponse, error) {
	debug(r, ">> requestVote", addr)
	pool := r.getConnPool(addr)
	conn, err := pool.getConn()
	if err != nil {
		return nil, err
	}
	resp := new(voteResponse)
	if err = conn.doRPC(rpcVote, req, resp); err != nil {
		_ = conn.close()
		return resp, err
	}
	pool.returnConn(conn)
	return resp, nil
}
