package raft

import (
	"testing"
)

func TestLdrShip_replicate_installSnap(t *testing.T) {
	t.Run("case1", func(t *testing.T) {
		testInstallSnapCase(t, false)
	})
	t.Run("case2", func(t *testing.T) {
		testInstallSnapCase(t, true)
	})
}

func testInstallSnapCase(t *testing.T, updateFSMAfterSnap bool) {
	// launch 3 node cluster
	c := newCluster(t)
	c.storeOpt.LogSegmentSize = 1024
	ldr, _ := c.ensureLaunch(3)
	defer c.shutdown()

	// send 100 updates, wait for them
	updates := uint64(30)
	<-c.sendUpdates(ldr, 1, 30).Done()

	// add two nonVoters M4; wait all commit them
	c.ensure(waitAddNonvoter(ldr, 4, id2Addr(4), false))
	c.waitCatchup()

	logCompacted := c.registerFor(logCompacted, ldr)
	defer c.unregister(logCompacted)

	// take snapshot
	c.takeSnapshot(ldr, 1, nil)

	// ensure log compacted
	c.ensure(logCompacted.waitForEvent(c.longTimeout))

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
