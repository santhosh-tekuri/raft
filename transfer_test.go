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
	"testing"
	"time"
)

// todo: add test for timeoutError
// todo: if ldr knows that a node is unreachable it should not try sending timeoutNow

// non leader should reply NotLeaderError for transfer request
func TestTransfer_nonLeader(t *testing.T) {
	// launch single node cluster
	c, _, flrs := launchCluster(t, 2)
	defer c.shutdown()

	_, err := waitTask(flrs[0], TransferLeadership(0, c.longTimeout), c.longTimeout)
	if _, ok := err.(NotLeaderError); !ok {
		c.Fatalf("err: got %v, want NotLeaderError", err)
	}
}

// in cluster with single voter, leader should reject transfer
// requests with ErrTransferNoVoter
func TestTransfer_noVoter(t *testing.T) {
	// launch single node cluster
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// launch new raft, and add him as nonvoter
	c.launch(1, false)
	c.waitCommitReady(ldr)
	c.ensure(c.waitAddNonvoter(ldr, 2, c.id2Addr(2), false))

	// transfer leadership, must return ErrTransferNoVoter
	_, err := waitTask(ldr, TransferLeadership(0, c.longTimeout), c.longTimeout)
	if err != ErrTransferNoVoter {
		c.Fatalf("err: got %v, want %v", err, ErrTransferNoVoter)
	}
}

func TestTransfer_nonVoter(t *testing.T) {
	// launch two node cluster
	c, ldr, _ := launchCluster(t, 2)
	defer c.shutdown()

	// launch new raft wit nid 3, and add him as nonvoter
	c.launch(1, false)
	c.waitCommitReady(ldr)
	c.ensure(c.waitAddNonvoter(ldr, 3, c.id2Addr(3), false))

	// transfer leadership to node 3, must return ErrTransferNonVoter
	_, err := waitTask(ldr, TransferLeadership(3, c.longTimeout), c.longTimeout)
	if err != ErrTransferTargetNonvoter {
		c.Fatalf("err: got %v, want %v", err, ErrTransferTargetNonvoter)
	}
}

func TestTransfer_invalidTarget(t *testing.T) {
	// launch two node cluster
	c, ldr, _ := launchCluster(t, 2)
	defer c.shutdown()

	// transfer leadership to unknown node 5, must return ErrTransferInvalidTarget
	_, err := waitTask(ldr, TransferLeadership(5, c.longTimeout), c.longTimeout)
	if err != ErrTransferInvalidTarget {
		c.Fatalf("err: got %v, want %v", err, ErrTransferInvalidTarget)
	}
}

func TestTransfer_self(t *testing.T) {
	// launch two node cluster
	c, ldr, _ := launchCluster(t, 2)
	defer c.shutdown()

	// transfer leadership to leader itself, must return ErrTransferSelf
	_, err := waitTask(ldr, TransferLeadership(ldr.nid, c.longTimeout), c.longTimeout)
	if err != ErrTransferSelf {
		c.Fatalf("err: got %v, want %v", err, ErrTransferSelf)
	}
}

// happy path: transfer leadership in 5 node cluster
func TestTransfer_anyTarget(t *testing.T) {
	doTransfer := func(t *testing.T, targetsReady bool) {
		// launch 5 node cluster
		c, ldr, _ := launchCluster(t, 5)
		defer c.shutdown()
		term := c.info(ldr).Term

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
		if got := c.info(newLdr).Term; got != term+1 {
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

func TestTransfer_givenTarget(t *testing.T) {
	doTransfer := func(t *testing.T, targetsReady bool) {
		// launch 5 node cluster
		c, ldr, flrs := launchCluster(t, 5)
		defer c.shutdown()
		term := c.info(ldr).Term

		c.sendUpdates(ldr, 1, 20)
		if targetsReady {
			c.waitFSMLen(20)
		}

		// transfer leadership, ensure no error
		c.ensure(waitTask(ldr, TransferLeadership(flrs[0].nid, c.longTimeout), c.longTimeout))

		// wait for new leader
		newLdr := c.waitForLeader()

		// check that leader is the target we gave
		if newLdr.nid != flrs[0].nid {
			c.Fatalf("newLeader=M%d, want M%d", newLdr.nid, flrs[0].nid)
		}

		// new leader term must be one greater than old leader term
		if got := c.info(newLdr).Term; got != term+1 {
			c.Fatalf("newLdr.term: got %d, want %d", got, term+1)
		}
	}

	t.Run("targetReady", func(t *testing.T) {
		doTransfer(t, true)
	})
	t.Run("targetNotReady", func(t *testing.T) {
		doTransfer(t, false)
	})
}

// launches 3 node cluster, with given quorumWait
// submits transferLeadership with given timeout
func setupTransferTimeout(t *testing.T, quorumWait, taskTimeout time.Duration) (c *cluster, ldr *Raft, flrs []*Raft, transfer Task) {
	// launch 3 node cluster, with given quorumWait
	c = newCluster(t)
	c.quorumWait = quorumWait
	ldr, flrs = c.ensureLaunch(3)

	// wait for bootstrap config committed by all
	c.waitForCommitted(c.info(ldr).LastLogIndex)

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
func TestTransfer_rejectAnotherTransferRequest(t *testing.T) {
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
func TestTransfer_rejectLogUpdateTasks(t *testing.T) {
	c, ldr, _, _ := setupTransferTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// send updateFSM request, must be rejected with InProgressError
	_, err := waitUpdate(ldr, "hello", 5*time.Millisecond)
	if _, ok := err.(InProgressError); !ok {
		t.Fatalf("err: got %#v, want InProgressError", err)
	}

	// send configChange request, must be rejected with InProgressError
	err = c.waitAddNonvoter(ldr, 5, c.id2Addr(5), false)
	if _, ok := err.(InProgressError); !ok {
		t.Fatalf("err: got %#v, want InProgressError", err)
	}
}

// if quorum became unreachable during transferLeadership,
// leader should reply ErrQuorumUnreachable
func TestTransfer_quorumUnreachable(t *testing.T) {
	c, _, _, transfer := setupTransferTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// transfer reply must be ErrQuorumUnreachable
	c.waitTaskDone(transfer, 2*time.Second, ErrQuorumUnreachable)
}

// if new term detected during transferLeadership before/after timeoutNow,
// leader should reply success
func TestTransfer_newTermDetected(t *testing.T) {
	c, ldr, flrs, transfer := setupTransferTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// send requestVote with one of the follower as candidate with new term
	testln("requestVote:", host(flrs[0]), "to", host(ldr))
	flrs[0].term++
	_, err := requestVote(flrs[0], ldr, true)
	if err != nil {
		c.Fatalf("requestVote: %v", err)
	}

	// transfer must succeed
	c.waitTaskDone(transfer, 2*time.Second, nil)
}

func TestTransfer_onShutdownReplyServerClosed(t *testing.T) {
	c, ldr, _, transfer := setupTransferTimeout(t, time.Second, 5*time.Second)
	defer c.shutdown()

	// shutdown ldr in background
	go c.shutdown(ldr)

	// transfer reply must be ErrServerClosed
	c.waitTaskDone(transfer, 2*time.Second, ErrServerClosed)
}
