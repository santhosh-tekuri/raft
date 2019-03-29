package raft

import (
	"testing"
)

func test_takeSnapshot_emptyFSM(t *testing.T) {
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// with nothing committed, asking for a snapshot should return an error
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
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// commit a log of things
	fsmLen := uint64(100)
	c.sendUpdates(ldr, 1, 100)
	c.waitBarrier(ldr, 0)

	// now take proper snapshot
	c.takeSnapshot(ldr, 10, nil)

	// ensure #snapshots is one
	if snaps := c.snaps(ldr); len(snaps) != 1 {
		t.Fatalf("numSnaps: got %v, want 1", snaps)
	}

	// log should have zero entries
	if got := c.inmemStorage(ldr).numEntries(); got != 0 {
		t.Fatalf("numEntries: got %d, want 0", got)
	}

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
