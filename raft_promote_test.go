package raft

import (
	"testing"
	"time"
)

func TestRaft_Promote(t *testing.T) {
	t.Run("newNode", test_promote_newNode)
	t.Run("existingNode", test_promote_existingNode)
}

// ---------------------------------------------------------

func test_promote_newNode(t *testing.T) {
	t.Run("singleRound", test_promote_newNode_singleRound)
	t.Run("uptodateButConfigChangeInProgress", test_promote_newNode_uptodateButConfigChangeInProgress)
	t.Run("multipleRound", test_promote_newNode_multipleRounds)
}

func test_promote_existingNode(t *testing.T) {
	t.Run("notUpToDate", test_promote_existingNode_notUpToDate)
	t.Run("upToDate", test_promote_existingNode_upToDate)
}

// ---------------------------------------------------------

// basically we are making 3 node cluster into 5 node cluster
func test_promote_newNode_singleRound(t *testing.T) {
	// create 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	promoting := c.registerFor(promoting, ldr)
	defer c.unregister(promoting)

	// add 2 new nodes with promote=true
	for _, id := range []ID{"M4", "M5"} {
		// launch new raft
		nr := c.launch(1, false)[id]

		// add him as nonvoter with promote=true
		c.ensure(waitAddNonVoter(ldr, id, id2Addr(id), true))

		// wait until leader promotes him to voter
		e, err := promoting.waitForEvent(c.longTimeout)
		if err != nil {
			t.Fatalf("waitForPromoting: %v", err)
		}
		if e.target != id {
			t.Fatalf("promoted: got %s, want %s", e.target, id)
		}
		if e.round != 1 {
			t.Fatalf("%s round: got %d, want %d", id, e.round, 1)
		}

		// wait for config commit, with new raft as voter
		isVoter := func() bool {
			n, ok := ldr.Info().Configs().Committed.Nodes[id]
			return ok && n.Voter
		}
		waitForCondition(isVoter, c.commitTimeout, c.longTimeout)

		// check that new node, knows that it is voter
		n, ok := nr.Info().Configs().Committed.Nodes[id]
		if !ok || !n.Voter {
			t.Fatalf("%s must have become voter", id)
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
	// create 2 node cluster, with long leaderLease
	c := newCluster(t)
	c.opt.LeaderLeaseTimeout = 10 * time.Second
	ldr, followers := c.ensureLaunch(2)
	defer c.shutdown()

	// shutdown the follower
	c.shutdown(followers[0])

	// add m3 as nonvoter with promote=true
	roundCompleted := c.registerFor(roundFinished, ldr)
	defer c.unregister(roundCompleted)
	promoting := c.registerFor(promoting, ldr)
	defer c.unregister(promoting)
	task := addNonVoter(ldr, "M3", id2Addr("M3"), true)
	select {
	case <-task.Done():
		t.Fatalf("should not be done: %v", task.Err())
	default:
	}

	// launch m3
	m3 := c.launch(1, false)["M3"]

	// wait until m4 ready for promotion
	e, err := roundCompleted.waitForEvent(c.longTimeout)
	if err != nil {
		t.Fatalf("waitForRoundComplete: %v", err)
	}
	if e.target != m3.id {
		t.Fatalf("roundCompleted: got %s, want %s", e.target, m3.id)
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
		t.Fatalf("promoted.src: got %s, want %s", e.src, ldr.id)
	}
	if e.target != m3.id {
		t.Fatalf("promoted.target: got %s, want %s", e.target, m3.id)
	}
	if e.round != 1 {
		t.Fatalf("%s round: got %d, want %d", m3.id, e.round, 1)
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
