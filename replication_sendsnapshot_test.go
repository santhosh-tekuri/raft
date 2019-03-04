package raft

import (
	"testing"

	"github.com/fortytw2/leaktest"
)

func TestReplication_SendSnapshot_followerWithNoSnapshotEntries(t *testing.T) {
	Debug("\nTestReplication_SendSnapshot_followerWithNoSnapshotEntries --------------------------")
	defer leaktest.Check(t)()

	// launch 5 node cluster
	c, ldr, followers := launchCluster(t, 5)
	defer c.shutdown()

	// shutdown 2 followers, say f1, f2
	f1, f2 := followers[0], followers[1]
	c.shutdown(f1, f2)

	// ensure quorum is not lost
	if state := ldr.Info().State(); state != Leader {
		t.Fatalf("leader became %s", state)
	}

	// send 10 updates, wait for them
	c.sendUpdates(ldr, 1, 10)
	c.waitBarrier(ldr, 0)

	// take snapshot
	c.takeSnapshot(ldr, 1, nil)
	_ = ldr.Info() // to ensure log compact finished
	if n := c.inmemStorage(ldr).numEntries(); n != 0 {
		c.Fatalf("ldr.numEntries: got %d, want 0", n)
	}

	f1 = c.restart(f1)
	c.waitFSMLen(10, f1)
	_ = ldr
}
