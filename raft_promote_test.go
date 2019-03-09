package raft

import (
	"testing"
)

func TestRaft_Promote(t *testing.T) {
	t.Run("newNode", test_promote_newNode)
	t.Run("existingNode", test_promote_existingNode)
}

// ---------------------------------------------------------

func test_promote_newNode(t *testing.T) {
	t.Run("singleRound", test_promote_newNode_singleRound)
	t.Run("multipleRound", test_promote_newNode_multipleRounds)
}

func test_promote_existingNode(t *testing.T) {
	t.Run("notUpToDate", test_promote_existingNode_notUpToDate)
	t.Run("upToDate", test_promote_existingNode_upToDate)
}

// ---------------------------------------------------------

func test_promote_newNode_singleRound(t *testing.T) {
	// create 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// add 2 new nodes with promote=true
	for _, id := range []ID{"M4", "M5"} {
		// launch new raft
		nr := c.launch(1, false)[id]

		// add him as nonvoter with promote=true
		promoting := c.registerFor(promoting, ldr)
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
