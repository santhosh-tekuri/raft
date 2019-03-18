package raft

import (
	"time"
)

type candShip struct {
	*Raft
	voteCh      chan rpcResponse
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
	c.voteCh = make(chan rpcResponse, len(c.configs.Latest.Nodes))

	// increment currentTerm
	c.setTerm(c.term + 1)

	debug(c, "startElection")
	if c.trace.ElectionStarted != nil {
		c.trace.ElectionStarted(c.liveInfo())
	}

	// send RequestVote RPCs to all other servers
	req := &voteReq{
		req:          req{c.term, c.nid},
		lastLogIndex: c.lastLogIndex,
		lastLogTerm:  c.lastLogTerm,
	}
	for _, n := range c.configs.Latest.Nodes {
		if !n.Voter {
			continue
		}
		if n.ID == c.nid {
			// vote for self
			c.setVotedFor(c.nid)
			c.voteCh <- rpcResponse{
				resp: rpcVote.createResp(c.Raft, success),
				from: c.nid,
			}
			continue
		}
		pool := c.getConnPool(n.ID)
		debug(c, n, ">>", req)
		go pool.doRPC(req, &voteResp{}, deadline, c.voteCh)
	}
}

func (c *candShip) onVoteResult(rpc rpcResponse) {
	if rpc.from != c.nid {
		debug(c, rpc)
	}
	if rpc.err != nil {
		return
	}

	// if response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if rpc.resp.getTerm() > c.term {
		c.setState(Follower)
		c.setTerm(rpc.resp.getTerm())
		return
	}

	// if votes received from majority of servers: become leader
	if rpc.resp.getResult() == success {
		c.votesNeeded--
		if c.votesNeeded == 0 {
			c.setState(Leader)
			c.setLeader(c.nid)
		}
	}
}
