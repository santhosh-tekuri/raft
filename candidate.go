// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import "time"

type candidate struct {
	*Raft
	respCh      chan rpcResponse
	votesNeeded int
	transfer    bool // to set voteReq.transfer
}

func (c *candidate) init()      { c.startElection() }
func (c *candidate) onTimeout() { c.startElection() }
func (c *candidate) release()   { c.respCh, c.transfer = nil, false }

func (c *candidate) startElection() {
	assert(c.configs.Latest.isVoter(c.nid))

	c.votesNeeded = c.configs.Latest.quorum()
	c.respCh = make(chan rpcResponse, len(c.configs.Latest.Nodes))

	// increment currentTerm and vote self
	c.setVotedFor(c.term+1, c.nid) // hit disk once
	c.respCh <- rpcResponse{
		response: rpcVote.createResp(c.Raft, success, nil),
		from:     c.nid,
	}

	if trace {
		println(c, "startElection")
	}
	d := c.rtime.duration(c.hbTimeout)
	deadline := time.Now().Add(d)
	c.timer.reset(d)
	if c.trace.ElectionStarted != nil {
		c.trace.ElectionStarted(c.liveInfo())
	}

	// send RequestVote RPCs to all other servers
	req := &voteReq{
		req:          req{c.term, c.nid},
		lastLogIndex: c.lastLogIndex,
		lastLogTerm:  c.lastLogTerm,
		transfer:     c.transfer,
	}
	for _, n := range c.configs.Latest.Nodes {
		if n.Voter && n.ID != c.nid {
			if trace {
				println(c, n, ">>", req)
			}
			pool := c.getConnPool(n.ID)
			go func(ch chan<- rpcResponse) {
				resp := &voteResp{}
				err := pool.doRPC(req, resp, deadline)
				ch <- rpcResponse{resp, pool.nid, err}
			}(c.respCh)
		}
	}
}

func (c *candidate) onVoteResult(resp rpcResponse) {
	if trace && resp.from != c.nid {
		println(c, resp)
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
