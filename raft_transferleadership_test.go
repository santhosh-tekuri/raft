package raft

import (
	"testing"
	"time"
)

// in cluster with single voter, leader should reject transferLeadership
// requests with ErrLeadershipTransferNoVoter
func test_transferLeadership_singleVoter(t *testing.T) {
	// launch single node cluster
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// launch new raft, and add him as nonvoter
	c.launch(1, false)
	c.ensure(waitAddNonVoter(ldr, 2, id2Addr(2), false))

	// transfer leadership, must return ErrLeadershipTransferNoVoter
	_, err := waitTask(ldr, TransferLeadership(c.longTimeout), c.longTimeout)
	if err != ErrLeadershipTransferNoVoter {
		c.Fatalf("err: got %v, want %v", err, ErrLeadershipTransferNoVoter)
	}
}

// happy path: transfer leadership in 5 node cluster
func test_transferLeadership_fiveNodes(t *testing.T) {
	// launch 5 node cluster
	c, ldr, _ := launchCluster(t, 5)
	defer c.shutdown()

	term := ldr.Info().Term()

	// transfer leadership, ensure no error
	c.ensure(waitTask(ldr, TransferLeadership(c.longTimeout), c.longTimeout))

	// wait for new leader
	newLdr := c.waitForLeader()

	// check leader is changed
	if ldr.ID() == newLdr.ID() {
		c.Fatal("no change in leader")
	}

	// new leader term must be one greater than old leader term
	if got := newLdr.Info().Term(); got != term+1 {
		c.Fatalf("newLdr.term: got %d, want %d", got, term+1)
	}
}

// if quorum became unreachable during transferLeadership,
// leader should reply ErrQuorumUnreachable
func test_transferLeadership_quorumUnreachable(t *testing.T) {
	// launch 3 node cluster, with quorumWait 1 sec
	c := newCluster(t)
	c.opt.QuorumWait = time.Second
	ldr, flrs := c.ensureLaunch(3)
	defer c.shutdown()

	// disconnect all followers
	c.shutdown(flrs...)

	// send an update, this makes sure that no transfer target is available
	ldr.NewEntries() <- UpdateFSM([]byte("test"))

	// request leadership transfer, with 5 sec timeout
	start := time.Now()
	_, err := waitTask(ldr, TransferLeadership(5*time.Second), c.longTimeout)

	// request must fail with ErrQuorumUnreachable
	if err != ErrQuorumUnreachable {
		t.Fatalf("err: got %v, want %v", err, ErrQuorumUnreachable)
	}

	// check that we got reply before specified timeout
	d := time.Now().Sub(start)
	if d > 2*time.Second {
		t.Fatalf("duration: got %v, want <5s", d)
	}
}
