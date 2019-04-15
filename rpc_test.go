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

import (
	"errors"
	"testing"
	"time"
)

// tests the behavior of voteReq when raft.leader!=0
func TestRaft_voteReq_leaderKnown(t *testing.T) {
	c, ldr, flrs := launchCluster(t, 3)
	defer c.shutdown()

	// wait until all nodes know who is leader
	c.waitForLeader()

	// a follower that thinks there's a leader should vote for that leader
	granted, err := requestVote(ldr, flrs[0], false)
	if err != nil {
		t.Fatalf("requestVote failed: %v", err)
	}
	if !granted {
		t.Fatalf("follower should grant vote to leader")
	}

	// a follower that thinks there's a leader shouldn't vote for a different candidate
	granted, err = requestVote(flrs[0], flrs[1], false)
	if err != nil {
		t.Fatalf("requestVote failed: %v", err)
	}
	if granted {
		t.Fatalf("follower should not grant vote candidate other than leader")
	}
}

func TestRPC_voteReq_opError(t *testing.T) {
	f := grantingVote
	failNow := make(chan struct{})
	grantingVote = func(s *storage, term, candidate uint64) error {
		if !isClosed(failNow) {
			return nil
		}
		if s.nid != candidate {
			return errors.New(t.Name())
		}
		return nil
	}
	defer func() { grantingVote = f }()

	c, ldr, flrs := launchCluster(t, 3)
	defer c.shutdown()

	shuttingDown := c.registerFor(eventShuttingDown, flrs...)
	defer c.unregister(shuttingDown)

	// make storage fail when voting other
	close(failNow)

	// shutdown leader, so that other two start election to chose new leader
	testln("shutting down leader", ldr.nid)
	c.shutdown(ldr)

	// ensure that who ever votes for other, shuts down
	// because of storage failure
	select {
	case e := <-shuttingDown.ch:
		flr := c.rr[e.src]
		if got := c.serveError(flr); got != e.err {
			t.Fatalf("serve=%v, want %v", got, e.err)
		}
	case <-time.After(c.longTimeout):
		t.Fatal("one of the follower is expected to shutdown")
	}
}
