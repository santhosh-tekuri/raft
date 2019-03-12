package raft

import (
	"time"
)

type candShip struct {
	*Raft
	voteCh      chan voteResult
	votesNeeded int
}

func (c *candShip) init()      { c.startElection() }
func (c *candShip) onTimeout() { c.startElection() }
func (c *candShip) release()   { c.voteCh = nil }

func (c *candShip) startElection() {
	d := c.rtime.duration(c.hbTimeout)
	c.timer.reset(d)
	deadline := time.Now().Add(d)
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
				voteResp: &voteResp{c.term, success},
				from:     c.id,
			}
			continue
		}
		connPool := c.getConnPool(n.ID)
		go func(ch chan<- voteResult) {
			resp, err := c.requestVote(connPool, req, deadline)
			ch <- voteResult{voteResp: resp, from: connPool.id, err: err}
		}(c.voteCh)
	}
}

func (c *candShip) requestVote(pool *connPool, req *voteReq, deadline time.Time) (*voteResp, error) {
	conn, err := pool.getConn()
	if err != nil {
		return nil, err
	}
	resp := new(voteResp)
	if c.trace.sending != nil {
		c.trace.sending(c.id, pool.id, Candidate, req)
	}
	_ = conn.conn.SetDeadline(deadline)
	if err = conn.doRPC(req, resp); err != nil {
		_ = conn.close()
		return nil, err
	}
	pool.returnConn(conn)
	if c.trace.received != nil {
		c.trace.received(c.id, pool.id, Candidate, req.term, resp)
	}
	return resp, nil
}

func (c *candShip) onVoteResult(v voteResult) {
	// todo: if quorum unreachable raise alert
	if v.err != nil {
		debug(c, "<< voteResp", v.from, v.err)
		return
	}

	// if response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if v.term > c.term {
		debug(c, "candidate -> follower")
		c.state = Follower
		c.setTerm(v.term)
		return
	}

	// if votes received from majority of servers: become leader
	if v.result == success {
		c.votesNeeded--
		if c.votesNeeded == 0 {
			debug(c, "candidate -> leader")
			c.state, c.leader = Leader, c.id
		}
	}
}

type voteResult struct {
	*voteResp
	from uint64
	err  error
}
