package raft

import (
	"testing"

	"github.com/fortytw2/leaktest"
)

func TestReplication_sendSnapshot(t *testing.T) {
	t.Run("case1", func(t *testing.T) {
		testReplicationSendSnapshot(t, false)
	})
	t.Run("case2", func(t *testing.T) {
		testReplicationSendSnapshot(t, true)
	})
}

func testReplicationSendSnapshot(t *testing.T, updateFSMAfterSnap bool) {
	Debug("\nTestReplication_sendSnapshot_", updateFSMAfterSnap, "--------------------------")
	defer leaktest.Check(t)()

	// launch 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// send 10 updates, wait for them
	updates := uint64(10)
	c.sendUpdates(ldr, 1, 10)
	c.waitBarrier(ldr, 0)

	// add two nonVoters M4; wait all commit them
	c.ensure(waitAddNonVoter(ldr, "M4", false))
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
	m4 := c.launch(1, false)["M4"]
	c.waitFSMLen(updates, m4)

	// send 10 more updates, ensure all alive get them
	updates += 10
	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(updates)
}
