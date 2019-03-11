package raft

import (
	"fmt"
	"testing"
	"time"
)

func test_voting(t *testing.T) {
	c, ldr, followers := launchCluster(t, 3)
	defer c.shutdown()

	// a follower that thinks there's a leader should vote for that leader
	granted, err := RequestVote(ldr, followers[0])
	if err != nil {
		t.Fatalf("requestVote failed: %v", err)
	}
	if !granted {
		t.Fatalf("voteGranted: got %t, want true", granted)
	}

	// a follower that thinks there's a leader shouldn't vote for a different candidate
	granted, err = RequestVote(followers[0], followers[1])
	if err != nil {
		t.Fatalf("requestVote failed: %v", err)
	} else if granted {
		t.Fatalf("voteGranted: got %t, want false", granted)
	}
}

func test_shutdown(t *testing.T) {
	c := newCluster(t)
	c.launch(1, true)

	// shutdown
	c.shutdown()

	// shutdown on stopped one, should work
	c.shutdown()
}

func test_bootstrap(t *testing.T) {
	c := newCluster(t)

	electionAborted := c.registerFor(electionAborted)
	defer c.unregister(electionAborted)

	// launch cluster without bootstrapping
	c.launch(3, false)
	defer c.shutdown()

	// all nodes should must abort election and only once
	timeout := time.After(c.longTimeout)
	aborted := make(map[ID]bool)
	for i := 0; i < 3; i++ {
		select {
		case e := <-electionAborted.ch:
			if aborted[e.src] {
				t.Fatalf("aborted twice")
			}
			aborted[e.src] = true
		case <-timeout:
			t.Fatal("timout in waiting for abort election")
		}
	}

	// bootstrap one of the nodes
	ldr := c.rr["M1"]
	nodes := make(map[ID]Node, 3)
	for _, r := range c.rr {
		nodes[r.ID()] = Node{ID: r.ID(), Addr: id2Addr(r.ID()), Voter: true}
	}
	if err := waitBootstrap(ldr, nodes, c.longTimeout); err != nil {
		t.Fatal(err)
	}

	// the bootstrapped node should be the leader
	c.waitForLeader(ldr)
	c.ensureLeader(ldr.ID())

	// should be able to apply
	if _, err := waitUpdate(ldr, "hello", 0); err != nil {
		t.Fatal(err)
	}
	c.waitFSMLen(1)
	c.ensureFSMSame([]string{"hello"})

	// ensure bootstrap fails if already bootstrapped
	if err := waitBootstrap(c.rr["M1"], nodes, c.longTimeout); err != ErrAlreadyBootstrapped {
		t.Fatalf("got %v, want %v", err, ErrAlreadyBootstrapped)
	}
	if err := waitBootstrap(c.rr["M2"], nodes, c.longTimeout); err != ErrAlreadyBootstrapped {
		t.Fatalf("got %v, want %v", err, ErrAlreadyBootstrapped)
	}

	// disconnect leader, and ensure that new leader is chosen
	c.disconnect(ldr)
	c.waitForLeader(c.exclude(ldr)...)
}

func test_singleNode(t *testing.T) {
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	// check response
	if resp.msg != "test" {
		t.Fatalf("apply response mismatch. got %s, want test", resp.msg)
	}

	// check index
	if resp.index != 1 {
		t.Fatalf("fsmReplyIndex: got %d want 1", resp.index)
	}
	if idx := fsm(ldr).len(); idx != 1 {
		t.Fatalf("fsm.len: got %d want 1", idx)
	}

	// check that it is applied to the FSM
	if cmd := fsm(ldr).lastCommand(); cmd != "test" {
		t.Fatalf("fsm.lastCommand: got %s want test", cmd)
	}

	// shutdown and restart with fresh fsm
	r := c.restart(ldr)

	// ensure that fsm has been restored from log
	c.waitFSMLen(fsm(ldr).len(), r)
	if cmd := fsm(r).lastCommand(); cmd != "test" {
		t.Fatalf("fsm.lastCommand: got %s want test", cmd)
	}
}

func test_tripleNode(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if resp.msg != "test" {
		t.Fatalf("apply response mismatch. got %s, want test", resp.msg)
	}
	if resp.index != 1 {
		t.Fatalf("fsmReplyIndex: got %d want 1", resp.index)
	}
	c.waitFSMLen(1)
}

func test_leaderFail(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if resp.msg != "test" {
		t.Fatalf("apply response mismatch. got %s, want test", resp.msg)
	}
	if resp.index != 1 {
		t.Fatalf("fsmReplyIndex: got %d want 1", resp.index)
	}
	c.waitFSMLen(1)

	// disconnect leader
	ldrTerm := ldr.Info().Term()
	c.disconnect(ldr)

	// leader should stepDown
	c.waitForState(ldr, c.longTimeout, Follower, Candidate)

	// wait for new leader
	newLdr := c.waitForLeader(c.exclude(ldr)...)

	// ensure leader term is greater
	if newLdrTerm := newLdr.Info().Term(); newLdrTerm <= ldrTerm {
		t.Fatalf("expected new leader term: newLdrTerm=%d, ldrTerm=%d", newLdrTerm, ldrTerm)
	}

	// apply should not work on old leader
	_, err = waitUpdate(ldr, "reject", c.heartbeatTimeout)
	if err, ok := err.(NotLeaderError); !ok {
		t.Fatalf("got %v, want NotLeaderError", err)
	} else if err.Leader != "" {
		t.Fatalf("got %s, want ", err.Leader)
	}

	// apply should work on new leader
	if _, err = waitUpdate(newLdr, "accept", c.heartbeatTimeout); err != nil {
		t.Fatalf("got %v, want nil", err)
	}

	// reconnect the networks
	c.connect()
	c.waitForHealthy()

	// wait for log replication
	c.waitFSMLen(2)

	// Check two entries are applied to the FSM
	c.ensureFSMSame([]string{"test", "accept"})
}

func test_behindFollower(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// disconnect one follower
	behind := c.followers()[0]
	c.disconnect(behind)

	// commit a lot of things
	for i := 0; i < 100; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("test%d", i)))
	}
	if _, err := waitUpdate(ldr, "test100", c.longTimeout); err != nil {
		t.Fatal(err)
	}

	// reconnect the behind node
	c.connect()
	c.waitForHealthy()

	// ensure all the logs are the same
	c.waitFSMLen(101)
	c.ensureFSMSame(nil)

	// Ensure one leader
	c.ensureLeader(c.leader().ID())
}

func test_leaderLeaseExpire(t *testing.T) {
	c, ldr, followers := launchCluster(t, 2)
	defer c.shutdown()

	// disconnect the follower now
	c.disconnect(followers[0])

	// the leader should stepDown within leaderLeaseTimeout
	c.waitForState(ldr, 2*c.heartbeatTimeout, Follower, Candidate)

	// should be no leaders
	if n := len(c.getInState(Leader)); n != 0 {
		t.Fatalf("#leaders: got %d, want 0", n)
	}

	// Ensure both have cleared their leader
	c.waitForState(followers[0], 2*c.heartbeatTimeout, Candidate)
	c.ensureLeader(ID(""))
}
