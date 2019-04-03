package raft

import "testing"

// tests that dialed conn is validated for cid and nid
func TestConnPool_getConn_IdentityError(t *testing.T) {
	// launch single node cluster
	c1, ldr, _ := launchCluster(t, 1)
	defer c1.shutdown()

	// start a raft instance without bootstrap with different cluster id
	c2 := newCluster(t)
	r2 := c2.launch(2, false)[2]
	defer c2.shutdown()

	// add node in cluster 2 as nonvoter in cluster1
	if err := waitAddNonvoter(ldr, 2, c2.id2Addr(2), false); err != nil {
		t.Fatal(err)
	}

	// ensure that ldr detects that nonvoter is does not belong to cluser
	// and treats it as unreachable
	_, err := c1.waitUnreachableDetected(ldr, r2)
	if _, ok := err.(IdentityError); !ok {
		c1.Fatalf("got %v, want IdentityError", err)
	}
}
