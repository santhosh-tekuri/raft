package raft

import (
	"testing"

	"github.com/fortytw2/leaktest"
)

func TestReplication_SendSnapshot_sendSnapshot2Follower(t *testing.T) {
	Debug("\nTestReplication_SendSnapshot_followerWithNoSnapshotEntries --------------------------")
	defer leaktest.Check(t)()

	// launch 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// send 10 updates, wait for them
	c.sendUpdates(ldr, 1, 10)
	c.waitBarrier(ldr, 0)

	// take snapshot
	c.takeSnapshot(ldr, 1, nil)
	_ = ldr.Info() // to ensure log compact finished
	if n := c.inmemStorage(ldr).numEntries(); n != 0 {
		c.Fatalf("ldr.numEntries: got %d, want 0", n)
	}

	// now add a nonVoter M4
	m4 := c.launch(1, false)["M4"]
	if err := waitAddNonVoter(ldr, m4.ID(), false); err != nil {
		c.Fatal(err)
	}

	// ensure M4 catches up with leader
	c.waitFSMLen(10, m4)

	debug("******************************")

	// send 10 more updates, ensure all alive get them
	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(20)
}
