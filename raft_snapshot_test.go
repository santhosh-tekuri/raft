package raft

import (
	"fmt"
	"testing"

	"github.com/fortytw2/leaktest"
)

func TestRaft_SnapshotRestore(t *testing.T) {
	Debug("\nTestRaft_SnapshotRestore --------------------------")
	defer leaktest.Check(t)()

	// launch 3 node cluster M1, M2, M3
	c := newCluster(t)
	c.launch(1, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()
	c.ensureLeader(ldr.ID())

	// with nothing committed, asking for a snapshot should return an error.
	takeSnap := TakeSnapshot(0)
	ldr.Tasks() <- takeSnap
	<-takeSnap.Done()
	if takeSnap.Err() != ErrNoUpdates {
		t.Fatalf("got %s, want ErrNoStateToSnapshot", takeSnap.Err())
	}

	// commit a log of things
	var ne NewEntry
	for i := 0; i < 1000; i++ {
		ne = UpdateEntry([]byte(fmt.Sprintf("msg-%d", i)))
		ldr.NewEntries() <- ne
	}
	<-ne.Done()
	if ne.Err() != nil {
		t.Fatal(ne.Err())
	}

	// if threshold is not reached, asking for a snapshot should return an error.
	takeSnap = TakeSnapshot(2000)
	ldr.Tasks() <- takeSnap
	<-takeSnap.Done()
	if takeSnap.Err() != ErrSnapshotThreshold {
		t.Fatalf("got %s, want ErrSnapshotThreshold", takeSnap.Err())
	}

	// now take proper snapshot
	takeSnap = TakeSnapshot(10)
	ldr.Tasks() <- takeSnap
	<-takeSnap.Done()
	if takeSnap.Err() != nil {
		t.Fatalf("got %s, want nil", takeSnap.Err())
	}

	// ensure #snapshots is one
	snaps, _ := ldr.snapshots.List()
	if len(snaps) != 1 {
		t.Fatalf("got %d, want 1", len(snaps))
	}

	// log should have zero entries
	count := uint64(111)
	waitInspect(ldr, func(info Info) {
		count = ldr.log.count()
	})
	if count != 0 {
		t.Fatalf("got %d, want 0", count)
	}

	// shutdown and restart with fresh fsm
	r := c.restart(ldr)

	// ensure that fsm has been restored from log
	c.waitFSMLen(fsm(ldr).len(), r)
	if cmd := fsm(r).lastCommand(); cmd != "msg-999" {
		t.Fatalf("fsm.lastCommand: got %s want test", cmd)
	}
}
