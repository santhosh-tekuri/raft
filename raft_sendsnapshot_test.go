package raft

import (
	"testing"
)

func test_sendSnapshot(t *testing.T) {
	t.Run("case1", func(t *testing.T) {
		test_sendSnapshot_case(t, false)
	})
	t.Run("case2", func(t *testing.T) {
		test_sendSnapshot_case(t, true)
	})
}

func test_sendSnapshot_case(t *testing.T, updateFSMAfterSnap bool) {
	// launch 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// send 10 updates, wait for them
	updates := uint64(10)
	c.sendUpdates(ldr, 1, 10)
	c.waitBarrier(ldr, 0)

	// add two nonVoters M4; wait all commit them
	c.ensure(waitAddNonVoter(ldr, 4, id2Addr(4), false))
	c.waitCatchup()

	// now send one update
	// why this: because snapIndex is determined by fsm
	//           and fsm does not know about config changes
	updates++
	c.ensure(waitUpdate(ldr, "msg:11", 0))

	// take snapshot
	c.takeSnapshot(ldr, 1, nil)
	_ = ldr.Info() // to ensure log compact finished
	if n := c.inmemStorage(ldr).numEntries(); n != 0 {
		c.Fatalf("ldr.numEntries: got %d, want 0", n)
	}

	if updateFSMAfterSnap {
		// send 10 more updates, ensure all alive get them
		updates += 10
		c.sendUpdates(ldr, 1, 10)
		c.waitFSMLen(updates)
	}

	// now launch nonVoter M4; ensure he catches up
	m4 := c.launch(1, false)[4]
	c.waitFSMLen(updates, m4)

	// send 10 more updates, ensure all alive get them
	updates += 10
	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(updates)
}
