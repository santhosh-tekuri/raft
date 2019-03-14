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
	c.ensure(waitAddNonvoter(ldr, 2, id2Addr(2), false))

	// transfer leadership, must return ErrLeadershipTransferNoVoter
	_, err := waitTask(ldr, TransferLeadership(c.longTimeout), c.longTimeout)
	if err != ErrLeadershipTransferNoVoter {
		c.Fatalf("err: got %v, want %v", err, ErrLeadershipTransferNoVoter)
	}
}

// happy path: transfer leadership in 5 node cluster
func test_transferLeadership_fiveNodes(t *testing.T, targetReady bool) {
	// launch 5 node cluster
	c, ldr, _ := launchCluster(t, 5)
	defer c.shutdown()

	term := ldr.Info().Term()

	c.sendUpdates(ldr, 1, 10)
	if targetReady {
		c.waitFSMLen(10)
	}

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

// launches 3 node cluster, with given quorumWait
// submits transferLeadership with given timeout
func setupTransferLeadershipTimeout(t *testing.T, quorumWait, taskTimeout time.Duration) (c *cluster, ldr *Raft, flrs []*Raft, transfer Task) {
	// launch 3 node cluster, with given quorumWait
	c = newCluster(t)
	c.opt.QuorumWait = quorumWait
	ldr, flrs = c.ensureLaunch(3)

	// shutdown all followers
	c.shutdown(flrs...)

	// send an update, this makes sure that no transfer target is available
	ldr.NewEntries() <- UpdateFSM([]byte("test"))

	// request leadership transfer, with given timeout,
	// this will not complete within this timeout
	transfer = TransferLeadership(taskTimeout)
	ldr.Tasks() <- transfer

	return
}

// leader should reject any transferLeadership requests,
// while one is already in progress
func test_transferLeadership_rejectAnotherTransferRequest(t *testing.T) {
	c, ldr, _, _ := setupTransferLeadershipTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// request another leadership transfer
	start := time.Now()
	_, err := waitTask(ldr, TransferLeadership(5*time.Second), c.longTimeout)

	// this new request must fail with InProgressError
	if _, ok := err.(InProgressError); !ok {
		t.Fatalf("err: got %#v, want InProgressError", err)
	}

	// check that we got InProgressError without any wait
	d := time.Now().Sub(start)
	if d > 5*time.Millisecond {
		t.Fatalf("duration: got %v, want <5s", d)
	}
}

// leader should reject any requests that update log,
// while transferLeadership is in progress
func test_transferLeadership_rejectLogUpdateTasks(t *testing.T) {
	c, ldr, _, _ := setupTransferLeadershipTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// send updateFSM request, must be rejected with InProgressError
	_, err := waitUpdate(ldr, "hello", 5*time.Millisecond)
	if _, ok := err.(InProgressError); !ok {
		t.Fatalf("err: got %#v, want InProgressError", err)
	}

	// send configChange request, must be rejected with InProgressError
	err = waitAddNonvoter(ldr, 5, id2Addr(5), false)
	if _, ok := err.(InProgressError); !ok {
		t.Fatalf("err: got %#v, want InProgressError", err)
	}
}

// if quorum became unreachable during transferLeadership,
// leader should reply ErrQuorumUnreachable
func test_transferLeadership_quorumUnreachable(t *testing.T) {
	c, _, _, transfer := setupTransferLeadershipTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	start := time.Now()
	select {
	case <-transfer.Done():
	case <-time.After(c.longTimeout):
		c.Fatal("transfer: timeout")
	}

	// transfer must fail with ErrQuorumUnreachable
	if err := transfer.Err(); err != ErrQuorumUnreachable {
		t.Fatalf("err: got %v, want %v", err, ErrQuorumUnreachable)
	}

	// check that we got reply before specified timeout
	d := time.Now().Sub(start)
	if d > 2*time.Second {
		t.Fatalf("duration: got %v, want <5s", d)
	}
}

// if new term detected during transferLeadership before/after timeoutNow,
// leader should reply success
func test_transferLeadership_newTermDetected(t *testing.T) {
	c, ldr, flrs, transfer := setupTransferLeadershipTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	start := time.Now()

	// send requestVote with one of the follower as candidate with new term
	flrs[0].term++
	_, err := RequestVote(flrs[0], ldr)
	if err != nil {
		c.Fatalf("requestVote: %v", err)
	}

	// wait for transferLeadership reply
	select {
	case <-transfer.Done():
	case <-time.After(c.longTimeout):
		c.Fatal("transferLeadership: timeout")
	}

	// reply must be success
	if transfer.Err() != nil {
		c.Fatalf("transferLeadership.Err: got %v, want nil", transfer.Err())
	}

	// check that we got reply before specified timeout
	d := time.Now().Sub(start)
	if d > 2*time.Second {
		t.Fatalf("duration: got %v, want <5s", d)
	}
}
