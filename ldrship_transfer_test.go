package raft

import (
	"testing"
	"time"
)

// todo: add test for timeoutError
// todo: if ldr knows that a node is unreachable it should not try sending timeoutNow

// in cluster with single voter, leader should reject transfer
// requests with ErrLeadershipTransferNoVoter
func TestLdrShip_transfer_singleVoter(t *testing.T) {
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
func TestLdrShip_transfer_fiveNodes(t *testing.T) {
	doTransfer := func(t *testing.T, targetsReady bool) {
		// launch 5 node cluster
		c, ldr, _ := launchCluster(t, 5)
		defer c.shutdown()
		term := ldr.Info().Term()

		c.sendUpdates(ldr, 1, 20)
		if targetsReady {
			c.waitFSMLen(20)
		}

		// transfer leadership, ensure no error
		c.ensure(waitTask(ldr, TransferLeadership(0, c.longTimeout), c.longTimeout))

		// wait for new leader
		newLdr := c.waitForLeader()

		// check leader is changed
		if ldr.NID() == newLdr.NID() {
			c.Fatal("no change in leader")
		}

		// new leader term must be one greater than old leader term
		if got := newLdr.Info().Term(); got != term+1 {
			c.Fatalf("newLdr.term: got %d, want %d", got, term+1)
		}
	}

	t.Run("targetsReady", func(t *testing.T) {
		doTransfer(t, true)
	})
	t.Run("targetsNotReady", func(t *testing.T) {
		doTransfer(t, false)
	})
}

// launches 3 node cluster, with given quorumWait
// submits transferLeadership with given timeout
func setupTransferTimeout(t *testing.T, quorumWait, taskTimeout time.Duration) (c *cluster, ldr *Raft, flrs []*Raft, transfer Task) {
	// launch 3 node cluster, with given quorumWait
	c = newCluster(t)
	c.opt.QuorumWait = quorumWait
	ldr, flrs = c.ensureLaunch(3)

	// wait for bootstrap config committed by all
	c.waitForCommitted(ldr.Info().LastLogIndex())

	// shutdown all followers
	c.shutdown(flrs...)

	// send an update, this makes sure that no transfer target is available
	ldr.FSMTasks() <- UpdateFSM([]byte("test"))

	// request leadership transfer, with given timeout,
	// this will not complete within this timeout
	transfer = TransferLeadership(0, taskTimeout)
	ldr.Tasks() <- transfer

	return
}

// leader should reject any transferLeadership requests,
// while one is already in progress
func TestLdrShip_transfer_rejectAnotherTransferRequest(t *testing.T) {
	c, ldr, _, _ := setupTransferTimeout(t, time.Second, 5*time.Second)
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
func TestLdrShip_transfer_rejectLogUpdateTasks(t *testing.T) {
	c, ldr, _, _ := setupTransferTimeout(t, time.Second, 5*time.Second)
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
func TestLdrShip_transfer_quorumUnreachable(t *testing.T) {
	c, _, _, transfer := setupTransferTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// transfer reply must be ErrQuorumUnreachable
	c.waitTaskDone(transfer, 2*time.Second, ErrQuorumUnreachable)
}

// if new term detected during transferLeadership before/after timeoutNow,
// leader should reply success
func TestLdrShip_transfer_newTermDetected(t *testing.T) {
	c, ldr, flrs, transfer := setupTransferTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// send requestVote with one of the follower as candidate with new term
	flrs[0].term++
	_, err := requestVote(flrs[0], ldr)
	if err != nil {
		c.Fatalf("requestVote: %v", err)
	}

	// transfer must succeed
	c.waitTaskDone(transfer, 2*time.Second, nil)
}

func TestLdrShip_transfer_onShutdownReplyServerClosed(t *testing.T) {
	c, ldr, _, transfer := setupTransferTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	tdebug("shutting down leader")
	ldr.Shutdown()

	// transfer reply must be ErrServerClosed
	c.waitTaskDone(transfer, 2*time.Second, ErrServerClosed)
}
