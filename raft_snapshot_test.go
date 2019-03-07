package raft

import (
	"testing"
)

func TestRaft_Snapshot_emptyFSM(t *testing.T) {
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// with nothing committed, asking for a snapshot should return an error
	c.takeSnapshot(ldr, 0, ErrNoUpdates)
}

func TestRaft_Snapshot_thresholdNotReached(t *testing.T) {
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// commit a log of things
	c.sendUpdates(ldr, 1, 1000)
	c.waitBarrier(ldr, 0)

	// if threshold is not reached, asking for a snapshot should return an error
	c.takeSnapshot(ldr, 2000, ErrSnapshotThreshold)
}

func TestRaft_Snapshot_restoreOnRestart(t *testing.T) {
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// commit a log of things
	c.sendUpdates(ldr, 1, 1000)
	c.waitBarrier(ldr, 0)

	// now take proper snapshot
	c.takeSnapshot(ldr, 10, nil)

	// wait for leader to finish compacting logs
	// note: compacting logs is done after replying takeSnapshot req
	ldr.Info()

	// ensure #snapshots is one
	if got := c.inmemStorage(ldr).numSnaps(); got != 1 {
		t.Fatalf("numSnaps: got %d, want 1", got)
	}

	// log should have zero entries
	if got := c.inmemStorage(ldr).numEntries(); got != 0 {
		t.Fatalf("numEntries: got %d, want 0", got)
	}

	// shutdown and restart with fresh fsm
	r := c.restart(ldr)

	// ensure that fsm has been restored from log
	c.waitFSMLen(fsm(ldr).len(), r)
	if cmd := fsm(r).lastCommand(); cmd != "update:1000" {
		t.Fatalf("fsm.lastCommand: got %s want update:1000", cmd)
	}
}
