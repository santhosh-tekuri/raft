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
	_, err := waitTask(ldr, TransferLeadership(0, c.longTimeout), c.longTimeout)
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
	c.ensure(waitTask(ldr, TransferLeadership(0, c.longTimeout), c.longTimeout))

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
	transfer = TransferLeadership(0, taskTimeout)
	ldr.Tasks() <- transfer

	return
}

// leader should reject any transferLeadership requests,
// while one is already in progress
func test_transferLeadership_rejectAnotherTransferRequest(t *testing.T) {
	c, ldr, _, _ := setupTransferLeadershipTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// request another leadership transfer
	_, err := waitTask(ldr, TransferLeadership(0, 5*time.Second), 5*time.Millisecond)

	// this new request must fail with InProgressError
	if _, ok := err.(InProgressError); !ok {
		t.Fatalf("err: got %#v, want InProgressError", err)
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

	// transfer reply must be ErrQuorumUnreachable
	c.waitTaskDone(transfer, 2*time.Second, ErrQuorumUnreachable)
}

// if new term detected during transferLeadership before/after timeoutNow,
// leader should reply success
func test_transferLeadership_newTermDetected(t *testing.T) {
	c, ldr, flrs, transfer := setupTransferLeadershipTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// send requestVote with one of the follower as candidate with new term
	flrs[0].term++
	_, err := RequestVote(flrs[0], ldr)
	if err != nil {
		c.Fatalf("requestVote: %v", err)
	}

	// transfer must succeed
	c.waitTaskDone(transfer, 2*time.Second, nil)
}

func test_transferLeadership_onShutdownReplyServerClosed(t *testing.T) {
	c, ldr, _, transfer := setupTransferLeadershipTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	tdebug("shutting down leader")
	ldr.Shutdown()

	// transfer reply must be ErrServerClosed
	c.waitTaskDone(transfer, 2*time.Second, ErrServerClosed)
}
