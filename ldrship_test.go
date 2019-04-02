package raft

import (
	"testing"
	"time"
)

func TestLdrShip_stepDown(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.longTimeout)
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
	_, err = waitUpdate(ldr, "reject", c.longTimeout)
	if err, ok := err.(NotLeaderError); !ok {
		t.Fatalf("got %v, want NotLeaderError", err)
	} else if err.LeaderAddr != "" {
		t.Fatalf("got %s, want ", err.LeaderAddr)
	}

	// apply should work on new leader
	if _, err = waitUpdate(newLdr, "accept", c.longTimeout); err != nil {
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

func TestLdrShip_quorumWait_unreachable(t *testing.T) {
	c := newCluster(t)
	c.opt.QuorumWait = 2 * time.Second
	ldr, followers := c.ensureLaunch(2)
	defer c.shutdown()

	unreachable := c.registerFor(unreachable, ldr)
	defer c.unregister(unreachable)
	quorumUnreachable := c.registerFor(quorumUnreachable, ldr)
	defer c.unregister(quorumUnreachable)

	// disconnect the follower now
	c.disconnect(followers[0])

	// wait for leader to detect
	c.ensure(unreachable.waitForEvent(c.heartbeatTimeout))
	start := time.Now()

	// check that we got quorumUnreachable trace
	e, err := quorumUnreachable.waitForEvent(c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("waitQuorumUnreachable: %v", err)
	}
	if e.since.IsZero() {
		t.Fatal("quorum must be unreachable")
	}
	if d := time.Now().Sub(start); d > 10*time.Millisecond {
		t.Fatalf("quorumUnreachable detection took %s", d)
	}
	start = time.Now()

	// wait for leader to stepDown
	c.waitForState(ldr, c.longTimeout, Follower, Candidate)

	// ensure that leader waited for quorumConfigured before stepDown
	if got := time.Now().Sub(start); got < 1*time.Second {
		t.Fatalf("quorumWait: got %s, want %s", got, c.opt.QuorumWait)
	}

	// should be no leaders
	if n := len(c.getInState(Leader)); n != 0 {
		t.Fatalf("#leaders: got %d, want 0", n)
	}

	// Ensure both have cleared their leader
	c.waitForState(followers[0], 2*c.heartbeatTimeout, Candidate)
	c.ensureLeader(0)
}

func TestLdrShip_quorumWait_reachable(t *testing.T) {
	c := newCluster(t)
	c.opt.QuorumWait = 30 * time.Minute
	ldr, followers := c.ensureLaunch(2)
	defer c.shutdown()

	unreachable := c.registerFor(unreachable, ldr)
	defer c.unregister(unreachable)
	quorumUnreachable := c.registerFor(quorumUnreachable, ldr)
	defer c.unregister(quorumUnreachable)

	// disconnect the follower now
	c.disconnect(followers[0])

	// wait for leader to detect
	c.ensure(unreachable.waitForEvent(c.heartbeatTimeout))
	start := time.Now()

	// check that we got quorumUnreachable trace
	e, err := quorumUnreachable.waitForEvent(c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("waitQuorumUnreachable: %v", err)
	}
	if e.since.IsZero() {
		t.Fatal("quorum must be unreachable")
	}
	if d := time.Now().Sub(start); d > 10*time.Millisecond {
		t.Fatalf("quorumUnreachable detection took %s", d)
	}

	// connect the follower now
	c.connect()

	// check that we got quorumReachable trace
	e, err = quorumUnreachable.waitForEvent(c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("waitQuorumReachable: %v", err)
	}
	if !e.since.IsZero() {
		t.Fatal("quorum must be reachable")
	}
}
