package raft

import (
	"testing"
	"time"
)

// basically we are making 3 node cluster into 5 node cluster
func test_promote_newNode_singleRound(t *testing.T) {
	// create 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	promoting := c.registerFor(promoting, ldr)
	defer c.unregister(promoting)

	// add 2 new nodes with promote=true
	for _, id := range []uint64{4, 5} {
		// launch new raft
		nr := c.launch(1, false)[id]

		// add him as nonvoter with promote=true
		c.ensure(waitAddNonvoter(ldr, id, id2Addr(id), true))

		// wait until leader promotes him to voter
		e, err := promoting.waitForEvent(c.longTimeout)
		if err != nil {
			t.Fatalf("waitForPromoting: %v", err)
		}
		if e.target != id {
			t.Fatalf("promoted: got M%d, want M%d", e.target, id)
		}
		if e.numRounds != 1 {
			t.Fatalf("M%d round: got %d, want %d", id, e.numRounds, 1)
		}

		// wait for config commit, with new raft as voter
		isVoter := func() bool {
			n, ok := ldr.Info().Configs().Committed.Nodes[id]
			return ok && n.Voter
		}
		if !waitForCondition(isVoter, c.commitTimeout, c.longTimeout) {
			c.Fatal("waitLdrConfigCommit: timeout")
		}

		// check that new node, knows that it is voter
		isVoter = func() bool {
			n, ok := nr.Info().Configs().Committed.Nodes[id]
			return ok && n.Voter
		}
		if !waitForCondition(isVoter, c.commitTimeout, c.longTimeout) {
			c.Fatalf("M%d must have become voter", id)
		}
	}

	// make sure that we have 5 voters in clusters
	configs := ldr.Info().Configs()
	if !configs.IsCommitted() {
		t.Fatal("config must be committed")
	}
	if got := configs.Latest.numVoters(); got != 5 {
		t.Fatalf("numVoters: got %d, want %d", got, 5)
	}
}

func test_promote_newNode_multipleRounds(t *testing.T) {
	t.Skip("test not implemented yet")
}

func test_promote_newNode_uptodateButConfigChangeInProgress(t *testing.T) {
	// create 2 node cluster, with long quorumWait
	c := newCluster(t)
	c.opt.QuorumWait = 10 * time.Second
	ldr, followers := c.ensureLaunch(2)
	defer c.shutdown()

	// shutdown the follower
	c.shutdown(followers[0])

	// add m3 as nonvoter with promote=true
	roundCompleted := c.registerFor(roundFinished, ldr)
	defer c.unregister(roundCompleted)
	promoting := c.registerFor(promoting, ldr)
	defer c.unregister(promoting)
	task := addNonvoter(ldr, 3, id2Addr(3), true)
	select {
	case <-task.Done():
		t.Fatalf("should not be done: %v", task.Err())
	default:
	}

	// launch m3
	m3 := c.launch(1, false)[3]

	// wait until m4 ready for promotion
	e, err := roundCompleted.waitForEvent(c.longTimeout)
	if err != nil {
		t.Fatalf("waitForRoundComplete: %v", err)
	}
	if e.target != m3.id {
		t.Fatalf("roundCompleted: got M%d, want M%d", e.target, m3.id)
	}

	// sleep a sec, to ensure that leader does not promote m3
	time.Sleep(time.Second)

	// ensure config is not committed, and m3 is still nonvoter
	configs := ldr.Info().Configs()
	if configs.IsCommitted() {
		t.Fatal("config should not be committed")
	}
	if configs.Latest.Nodes[m3.id].Voter {
		t.Fatal("m3 must still be nonvoter")
	}

	// now launch follower with longer hbtimeout, so that
	// he does not start election and leader has time to contact him
	c.opt.HeartbeatTimeout = 20 * time.Second
	c.restart(followers[0])

	// wait until leader promotes m3 to voter, with just earlier round
	e, err = promoting.waitForEvent(c.longTimeout)
	if err != nil {
		t.Fatalf("waitForPromoting: %v", err)
	}
	if e.src != ldr.id {
		t.Fatalf("promoted.src: got M%d, want M%d", e.src, ldr.id)
	}
	if e.target != m3.id {
		t.Fatalf("promoted.target: got M%d, want M%d", e.target, m3.id)
	}
	if e.numRounds != 1 {
		t.Fatalf("M%d round: got %d, want %d", m3.id, e.numRounds, 1)
	}

	// wait for config commit, with m3 as voter
	isVoter := func() bool {
		n, ok := ldr.Info().Configs().Committed.Nodes[m3.id]
		return ok && n.Voter
	}
	waitForCondition(isVoter, c.commitTimeout, c.longTimeout)
}

// ---------------------------------------------------------

func test_promote_existingNode_notUpToDate(t *testing.T) {
	t.Skip("test not implemented yet")
}

func test_promote_existingNode_upToDate(t *testing.T) {
	t.Run("configCommitted", test_promote_existingNode_upToDate_configCommitted)
	t.Run("configNotCommitted", test_promote_existingNode_upToDate_configNotCommitted)
}

// ---------------------------------------------------------

func test_promote_existingNode_upToDate_configCommitted(t *testing.T) {
	t.Skip("test not implemented yet")
}

func test_promote_existingNode_upToDate_configNotCommitted(t *testing.T) {
	t.Skip("test not implemented yet")
}
