package raft

type candShip struct {
	*Raft
	respCh      chan rpcResponse
	votesNeeded int
}

func (c *candShip) init()      { c.startElection() }
func (c *candShip) onTimeout() { c.startElection() }
func (c *candShip) release()   { c.respCh = nil }

func (c *candShip) startElection() {
	d := c.rtime.duration(c.hbTimeout)
	c.timer.reset(d)
	c.votesNeeded = c.configs.Latest.quorum()
	c.respCh = make(chan rpcResponse, len(c.configs.Latest.Nodes))

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
			c.respCh <- rpcResponse{
				response: rpcVote.createResp(c.Raft, success, nil),
				from:     c.nid,
			}
			continue
		}
		pool := c.getConnPool(n.ID)
		debug(c, n, ">>", req)
		go func(ch chan<- rpcResponse) {
			resp := &voteResp{}
			err := pool.doRPC(req, resp)
			ch <- rpcResponse{resp, pool.nid, err}
		}(c.respCh)
	}
}

func (c *candShip) onVoteResult(resp rpcResponse) {
	if resp.from != c.nid {
		debug(c, resp)
	}
	if resp.err != nil {
		return
	}

	// if response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if resp.getTerm() > c.term {
		c.setState(Follower)
		c.setTerm(resp.getTerm())
		return
	}

	// if votes received from majority of servers: become leader
	if resp.getResult() == success {
		c.votesNeeded--
		if c.votesNeeded == 0 {
			c.setState(Leader)
			c.setLeader(c.nid)
		}
	}
}
