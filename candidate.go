package raft

import "time"

func (r *Raft) runCandidate() {
	c := candShip{Raft: r}
	c.init()
	c.runLoop()
}

type candShip struct {
	*Raft
	timeoutCh   <-chan time.Time
	voteCh      chan voteResult
	votesNeeded int
}

func (c *candShip) init() {
	c.startElection()
}

func (c *candShip) release() {
	c.timeoutCh = nil
	c.voteCh = nil
}

func (c *candShip) runLoop() {
	assert(c.leader == "", "%s r.leader: got %s, want ", c, c.leader)
	for c.state == Candidate {
		select {
		case <-c.shutdownCh:
			return

		case rpc := <-c.server.rpcCh:
			c.replyRPC(rpc)

		case vote := <-c.voteCh:
			c.onVoteResult(vote)

		case <-c.timeoutCh:
			c.startElection()

		case ne := <-c.newEntryCh:
			ne.reply(NotLeaderError{c.leaderAddr(), false})

		case t := <-c.taskCh:
			c.executeTask(t)

		case t := <-c.snapTakenCh:
			c.onSnapshotTaken(t)
		}
	}
}

func (c *candShip) startElection() {
	c.timeoutCh = afterRandomTimeout(c.hbTimeout)
	c.votesNeeded = c.configs.Latest.quorum()
	c.voteCh = make(chan voteResult, len(c.configs.Latest.Nodes))

	// increment currentTerm
	c.setTerm(c.term + 1)

	debug(c, "startElection")
	if c.trace.ElectionStarted != nil {
		c.trace.ElectionStarted(c.liveInfo())
	}

	// send RequestVote RPCs to all other servers
	req := &voteReq{
		term:         c.term,
		candidate:    c.id,
		lastLogIndex: c.lastLogIndex,
		lastLogTerm:  c.lastLogTerm,
	}
	for _, n := range c.configs.Latest.Nodes {
		if !n.Voter {
			continue
		}
		if n.ID == c.id {
			// vote for self
			c.setVotedFor(c.id)
			c.voteCh <- voteResult{
				voteResp: &voteResp{
					term:    c.term,
					granted: true,
				},
				from: c.id,
			}
			continue
		}
		connPool := c.getConnPool(n.ID)
		go func() {
			result := voteResult{
				voteResp: &voteResp{
					term:    req.term,
					granted: false,
				},
				from: connPool.id,
			}
			defer func() {
				c.voteCh <- result
			}()
			resp, err := c.requestVote(connPool, req)
			if err != nil {
				result.err = err
				return
			}
			result.voteResp = resp
		}()
	}
}

func (c *candShip) requestVote(pool *connPool, req *voteReq) (*voteResp, error) {
	debug(c.id, ">> requestVote", pool.id)
	conn, err := pool.getConn()
	if err != nil {
		return nil, err
	}
	resp := new(voteResp)
	if c.trace.sending != nil {
		c.trace.sending(c.id, pool.id, req)
	}
	if err = conn.doRPC(req, resp); err != nil {
		_ = conn.close()
		return nil, err
	}
	pool.returnConn(conn)
	if c.trace.received != nil {
		c.trace.received(c.id, pool.id, resp)
	}
	return resp, nil
}

func (c *candShip) onVoteResult(vote voteResult) {
	// todo: if quorum unreachable raise alert
	if vote.from != c.id {
		debug(c, "<< voteResp", vote.from, vote.granted, vote.term, vote.err)
	}

	if vote.err != nil {
		return
	}
	// if response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if vote.term > c.term {
		debug(c, "candidate -> follower")
		c.state = Follower
		c.setTerm(vote.term)
		c.stateChanged()
		return
	}

	// if votes received from majority of servers: become leader
	if vote.granted {
		c.votesNeeded--
		if c.votesNeeded == 0 {
			debug(c, "candidate -> leader")
			c.state = Leader
			c.leader = c.id
			c.stateChanged()
		}
	}
}

type voteResult struct {
	*voteResp
	from ID
	err  error
}
