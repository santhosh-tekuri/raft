// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"reflect"
	"testing"
	"time"
)

func TestChangeConfig_validations(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// wait until leader is commit ready
	c.waitCommitReady(ldr)

	configs := c.info(ldr).Configs

	// adding node with empty id should fail
	if err := c.waitAddNonvoter(ldr, 0, "localhost:8888", false); err == nil {
		t.Fatal(err)
	}

	// adding voter to bootstrapped config should fail
	if err := configs.Latest.AddVoter(4, "localhost:2222"); err == nil {
		t.Fatal(err)
	}

	// setting action on non existing node should fail
	if err := configs.Latest.SetAction(4, Promote); err == nil {
		t.Fatal(err)
	}

	// promoting voter should fail
	if err := configs.Latest.SetAction(3, Promote); err == nil {
		t.Fatal(err)
	}

	// setting addr on non existing node should fail
	if err := configs.Latest.SetAddr(4, "localhost:2222"); err == nil {
		t.Fatal(err)
	}

	// setting invalid addr should fail
	if err := configs.Latest.SetAddr(3, "localhost"); err == nil {
		t.Fatal(err)
	}

	// setting existing addr should fail
	if err := configs.Latest.SetAddr(3, c.id2Addr(2)); err == nil {
		t.Fatal(err)
	}

	// setting data on non existing node should fail
	if err := configs.Latest.SetData(4, "localhost:2222"); err == nil {
		t.Fatal(err)
	}

	// adding node with empty addr should fail
	if err := c.waitAddNonvoter(ldr, 10, "", false); err == nil {
		t.Fatal(err)
	}

	// adding node with existing id should fail
	config := c.info(ldr).Configs.Latest
	for id := range config.Nodes {
		if err := config.AddNonvoter(id, "localhost:8888", false); err == nil {
			t.Fatal(err)
		}
	}

	// adding node with existing addr should fail
	for _, n := range c.info(ldr).Configs.Latest.Nodes {
		if err := c.waitAddNonvoter(ldr, 12, n.Addr, false); err == nil {
			t.Fatal(err)
		}
	}

	// ensure that config is not changed because of above errors
	if configsNow := c.info(ldr).Configs; !reflect.DeepEqual(configsNow, configs) {
		t.Log("old: ", configs)
		t.Log("new: ", configsNow)
		t.Fatal("configs changed")
	}
}

func TestChangeConfig_trace(t *testing.T) {
	// launch 2 node cluster M1, M2
	c, ldr, followers := launchCluster(t, 2)
	defer c.shutdown()

	// wait until leader is commit ready
	c.waitCommitReady(ldr)

	// wait for bootstrap config committed by all
	c.waitForCommitted(c.info(ldr).LastLogIndex)

	configRelated := c.registerFor(eventConfigRelated)
	defer c.unregister(configRelated)

	// add M3 as nonvoter, wait for success reply
	c.ensure(c.waitAddNonvoter(ldr, 3, c.id2Addr(3), false))

	// ensure that leader raised configChange
	select {
	case e := <-configRelated.ch:
		if e.src != ldr.NID() {
			t.Fatalf("got M%d, want M%d", e.src, ldr.NID())
		}
		if e.typ != eventConfigChanged {
			t.Fatalf("got %d, want %d", e.typ, eventConfigChanged)
		}
	default:
		t.Fatal("expected configChange from ldr")
	}

	// ensure that follower raised configChange
	select {
	case e := <-configRelated.ch:
		if e.src != followers[0].NID() {
			t.Fatalf("got M%d, want M%d", e.src, followers[0].NID())
		}
		if e.typ != eventConfigChanged {
			t.Fatalf("got %d, want %d", e.typ, eventConfigChanged)
		}
	default:
		t.Fatal("expected configChange from follower")
	}

	// ensure that leader raised configCommitted
	select {
	case e := <-configRelated.ch:
		if e.src != ldr.NID() {
			t.Fatalf("got M%d, want M%d", e.src, ldr.NID())
		}
		if e.typ != eventConfigCommitted {
			t.Fatalf("got %d, want %d", e.typ, eventConfigCommitted)
		}
	default:
		t.Fatal("expected configCommitted from ldr")
	}

	// wait and ensure that followers raised configCommitted
	c.waitForCommitted(c.info(ldr).LastLogIndex, followers[0])
	select {
	case e := <-configRelated.ch:
		if e.src != followers[0].NID() {
			t.Fatalf("got M%d, want M%d", e.src, followers[0].NID())
		}
		if e.typ != eventConfigCommitted {
			t.Fatalf("got %d, want %d", e.typ, eventConfigCommitted)
		}
	default:
		t.Fatal("expected configCommit from follower")
	}

	// launch new raft instance M3, without bootstrap
	m3 := c.launch(1, false)[3]

	// ensure that config committed by all
	c.waitForCommitted(c.info(ldr).LastLogIndex, m3)
	for _, r := range c.rr {
		info := c.info(r)
		if !info.Configs.IsCommitted() {
			t.Fatalf("config is not committed by M%d %s", info.NID, info.State)
		}
		m3, ok := info.Configs.Committed.Nodes[3]
		if !ok {
			t.Fatalf("m3 is not present in M%d %s", info.NID, info.State)
		}
		if m3.Voter {
			t.Fatalf("m3 must be nonvoter in M%d %s", info.NID, info.State)
		}
	}
}

// basically we are making 3 node cluster into 5 node cluster
func TestChangeConfig_promote_newNode_singleRound(t *testing.T) {
	// create 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// wait until leader is commit ready
	c.waitCommitReady(ldr)

	promoting := c.registerFor(eventConfigActionStarted, ldr)
	defer c.unregister(promoting)

	// add 2 new nodes with promote=true
	for _, id := range []uint64{4, 5} {
		// launch new raft
		nr := c.launch(1, false)[id]

		// add him as nonvoter with promote=true
		c.ensure(c.waitAddNonvoter(ldr, id, c.id2Addr(id), true))

		// wait until leader promotes him to voter
		e, err := promoting.waitForEvent(c.longTimeout)
		if err != nil {
			t.Fatalf("waitForPromoting: %v", err)
		}
		if e.target != id {
			t.Fatalf("promoted: got M%d, want M%d", e.target, id)
		}
		if e.action != Promote {
			t.Fatalf("configAction: got %v, want Promote", e.action)
		}
		if e.numRounds != 1 {
			t.Fatalf("M%d round: got %d, want %d", id, e.numRounds, 1)
		}

		// wait for stable configuration
		c.waitForStableConfig(ldr)

		// check that new node is voter
		if !c.info(ldr).Configs.Committed.isVoter(id) {
			t.Fatalf("new node is not voter")
		}

		// check that new node, knows that it is voter
		isVoter := func() bool {
			n, ok := c.info(nr).Configs.Committed.Nodes[id]
			return ok && n.Voter
		}
		if !waitForCondition(isVoter, c.commitTimeout, c.longTimeout) {
			c.Fatalf("M%d must have become voter", id)
		}
	}

	// make sure that we have 5 voters in clusters
	configs := c.info(ldr).Configs
	if !configs.IsCommitted() {
		t.Fatal("config must be committed")
	}
	if got := configs.Latest.numVoters(); got != 5 {
		t.Fatalf("numVoters: got %d, want %d", got, 5)
	}
}

// todo: test promote newNode multipleRounds

func TestChangeConfig_promote_newNode_uptodateButConfigChangeInProgress(t *testing.T) {
	// create 2 node cluster, with long quorumWait
	c := newCluster(t)
	c.quorumWait = 10 * time.Second
	ldr, followers := c.ensureLaunch(2)
	defer c.shutdown()

	// wait until leader is commit ready
	c.waitCommitReady(ldr)

	// wait for bootstrap config committed by all
	c.waitForCommitted(c.info(ldr).LastLogIndex)

	// shutdown the follower
	c.shutdown(followers[0])

	// add m3 as nonvoter with promote=true
	roundCompleted := c.registerFor(eventRoundFinished, ldr)
	defer c.unregister(roundCompleted)
	promoting := c.registerFor(eventConfigActionStarted, ldr)
	defer c.unregister(promoting)
	task := c.addNonvoter(ldr, 3, c.id2Addr(3), true)
	select {
	case <-task.Done():
		t.Fatalf("should not be done: %v", task.Err())
	default:
	}

	// launch m3
	m3 := c.launch(1, false)[3]

	// wait until m3 ready for promotion
	testln("waitRoundCompleted")
	e, err := roundCompleted.waitForEvent(0)
	if err != nil {
		t.Fatalf("waitForRoundComplete: %v", err)
	}
	if e.target != m3.nid {
		t.Fatalf("roundCompleted: got M%d, want M%d", e.target, m3.nid)
	}

	// sleep a sec, to ensure that leader does not promote m3
	time.Sleep(time.Second)

	// ensure config is not committed, and m3 is still nonvoter
	configs := c.info(ldr).Configs
	if configs.IsCommitted() {
		t.Fatal("config should not be committed")
	}
	if configs.Latest.Nodes[m3.nid].Voter {
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
	if e.src != ldr.nid {
		t.Fatalf("promoted.src: got M%d, want M%d", e.src, ldr.nid)
	}
	if e.target != m3.nid {
		t.Fatalf("promoted.target: got M%d, want M%d", e.target, m3.nid)
	}
	if e.action != Promote {
		t.Fatalf("configAction: got %v, want Promote", e.action)
	}
	if e.numRounds != 1 {
		t.Fatalf("M%d round: got %d, want %d", m3.nid, e.numRounds, 1)
	}

	// wait for config commit, with m3 as voter
	isVoter := func() bool {
		n, ok := c.info(ldr).Configs.Committed.Nodes[m3.nid]
		return ok && n.Voter
	}
	waitForCondition(isVoter, c.commitTimeout, c.longTimeout)
}

// tests that we can convert 5 node cluster into two node cluster with single ChangeConfig
func TestChangeConfig_removeVoters(t *testing.T) {
	// launch 5 node cluster
	c, ldr, flrs := launchCluster(t, 5)
	defer c.shutdown()

	// wait for commit ready
	c.waitCommitReady(ldr)

	electionAborted0 := c.registerFor(eventElectionAborted, flrs[0])
	defer c.unregister(electionAborted0)
	electionAborted1 := c.registerFor(eventElectionAborted, flrs[1])
	defer c.unregister(electionAborted1)

	// submit ChangeConfig with two voters removed
	config := c.info(ldr).Configs.Latest
	if err := config.SetAction(flrs[0].nid, Remove); err != nil {
		t.Fatal(err)
	}
	if err := config.SetAction(flrs[1].nid, Remove); err != nil {
		t.Fatal(err)
	}
	c.ensure(waitTask(ldr, ChangeConfig(config), c.longTimeout))

	// wait for stable config
	c.ensure(waitTask(ldr, WaitForStableConfig(), c.longTimeout))

	// ensure that removed nodes aborted election
	e, err := electionAborted0.waitForEvent(c.longTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if e.reason != "not voter" {
		c.Fatalf("reason=%q, want %q", e.reason, "not part of cluster")
	}
	_, err = electionAborted1.waitForEvent(c.longTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if e.reason != "not voter" {
		c.Fatalf("reason=%q, want %q", e.reason, "not part of cluster")
	}

	// shutdown the removed nodes
	c.shutdown(flrs[0], flrs[1])

	// shutdown the leader
	c.shutdown(ldr)

	// wait for leader among the remaining two nodes
	c.waitForLeader(flrs[2], flrs[3])
}

func TestChangeConfig_removeLeader(t *testing.T) {
	// launch 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// wait for commit ready
	c.waitCommitReady(ldr)

	// submit ChangeConfig with leader removed
	config := c.info(ldr).Configs.Latest
	if err := config.SetAction(ldr.nid, Remove); err != nil {
		t.Fatal(err)
	}
	c.ensure(waitTask(ldr, ChangeConfig(config), c.longTimeout))

	// the leader must have become follower
	if s := c.getState(ldr); s != Follower {
		t.Fatalf("state=%v, want %v", s, Follower)
	}

	// wait for new leader
	newLdr := c.waitForLeader()

	// ensure that leader is not same
	if ldr.nid == newLdr.nid {
		t.Fatal()
	}

	// check old leader is shutdown
	if !ldr.isClosed() {
		t.Fatalf("leader is not shutdown after removal")
	}

	// check shutdown reason was ErrNodeRemoved
	if got := c.serveError(ldr); got != ErrNodeRemoved {
		t.Fatalf("serve=%v, want ErrNodeRemoved", got)
	}

	// restart old leader and check that it aborts election with
	// reason "not part of cluster"
	electionAborted := c.registerFor(eventElectionAborted, ldr)
	defer c.unregister(electionAborted)
	c.restart(ldr)
	select {
	case e := <-electionAborted.ch:
		if e.reason != "not part of cluster" {
			t.Fatalf("electionAborted=%s, want %s", e.reason, "not part of cluster")
		}
	case <-time.After(c.longTimeout):
		t.Fatal("no electionAborted detected")
	}
}

func TestChangeConfig_demoteLeader(t *testing.T) {
	// launch 3 node cluster
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// wait for commit ready
	c.waitCommitReady(ldr)

	// submit ChangeConfig with leader demoted
	config := c.info(ldr).Configs.Latest
	if err := config.SetAction(ldr.nid, Demote); err != nil {
		t.Fatal(err)
	}
	c.ensure(waitTask(ldr, ChangeConfig(config), c.longTimeout))

	// the leader must have become follower
	if s := c.getState(ldr); s != Follower {
		t.Fatalf("state=%v, want %v", s, Follower)
	}

	// wait for new leader
	newLdr := c.waitForLeader()

	// ensure that leader is not same
	if ldr.nid == newLdr.nid {
		t.Fatal()
	}

	// restart old leader and check that it aborts election with
	// reason "not voter"
	electionAborted := c.registerFor(eventElectionAborted, ldr)
	defer c.unregister(electionAborted)
	c.restart(ldr)
	select {
	case e := <-electionAborted.ch:
		if e.reason != "not voter" {
			t.Fatalf("electionAborted=%s, want %s", e.reason, "not voter")
		}
	case <-time.After(c.longTimeout):
		t.Fatal("no electionAborted detected")
	}
}

// ---------------------------------------------------------

// todo: test promote existingNode notUptodate

// todo: test promote existingNode uptodate
//       - configCommitted
//       - configNotCommitted
