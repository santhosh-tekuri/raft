package raft

import (
	"errors"
	"testing"
	"time"
)

func TestRPC_voteReq_opError(t *testing.T) {
	f := grantingVote
	failNow := make(chan struct{})
	grantingVote = func(s *storage, term, candidate uint64) error {
		if !isClosed(failNow) {
			return nil
		}
		if s.nid != candidate {
			return errors.New(t.Name())
		}
		return nil
	}
	defer func() { grantingVote = f }()

	c, ldr, flrs := launchCluster(t, 3)
	defer c.shutdown()

	shuttingDown := c.registerFor(shuttingDown, flrs...)
	defer c.unregister(shuttingDown)

	// make storage fail when voting other
	close(failNow)

	// shutdown leader, so that other two start election to chose new leader
	tdebug("shutting down leader", ldr.nid)
	c.shutdown(ldr)

	// ensure that who ever votes for other, shuts down
	// because of storage failure
	select {
	case e := <-shuttingDown.ch:
		c.shutdownErr(false, c.rr[e.src])
	case <-time.After(c.longTimeout):
		t.Fatal("one of the follower is expected to shutdown")
	}
}
