package raft

import (
	"testing"
)

func test_takeSnapshot_emptyLog(t *testing.T) {
	c := newCluster(t)
	ldr := c.launch(1, false)[1]
	defer c.shutdown()

	// with empty log, asking for a snapshot should return an error
	c.takeSnapshot(ldr, 0, ErrNoUpdates)
}

func test_takeSnapshot_thresholdNotReached(t *testing.T) {
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// commit a log of things
	c.sendUpdates(ldr, 1, 1000)
	c.waitBarrier(ldr, 0)

	// if threshold is not reached, asking for a snapshot should return an error
	c.takeSnapshot(ldr, 2000, ErrSnapshotThreshold)
}

func test_takeSnapshot_restartSendUpdates(t *testing.T) {
	c := newCluster(t)
	c.storeOpt.LogSegmentSize = 1024
	ldr, _ := c.ensureLaunch(1)
	defer c.shutdown()

	// commit a log of things
	fsmLen := uint64(100)
	c.sendUpdates(ldr, 1, 100)
	c.waitBarrier(ldr, 0)

	logCompacted := c.registerFor(logCompacted, ldr)
	defer c.unregister(logCompacted)

	// now take proper snapshot
	c.takeSnapshot(ldr, 10, nil)

	// ensure #snapshots is one
	if snaps := c.snaps(ldr); len(snaps) != 1 {
		t.Fatalf("numSnaps: got %v, want 1", snaps)
	}

	// ensure log compacted
	c.ensure(logCompacted.waitForEvent(c.longTimeout))

	// shutdown and restart with fresh fsm
	r := c.restart(ldr)
	c.waitForLeader(r)

	// ensure that fsm has been restored from log
	c.waitFSMLen(fsmLen, r)
	if cmd := fsm(r).lastCommand(); cmd != "update:100" {
		t.Fatalf("fsm.lastCommand: got %s want update:100", cmd)
	}

	// send few updates and ensure they reach fsm
	c.sendUpdates(r, 1, 3)
	c.waitFSMLen(fsmLen+3, r)
}
