package raft

import (
	"reflect"
	"testing"
	"time"
)

func test_changeConfig_validations(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	configs := ldr.Info().Configs()

	// adding node with empty id should fail
	if err := waitAddNonvoter(ldr, 0, "localhost:8888", false); err == nil {
		t.Fatal(err)
	}

	// adding node with empty addr should fail
	if err := waitAddNonvoter(ldr, 10, "", false); err == nil {
		t.Fatal(err)
	}

	// adding node with existing id should fail
	config := ldr.Info().Configs().Latest
	for id := range config.Nodes {
		if err := config.AddNonVoter(id, "localhost:8888", false); err == nil {
			t.Fatal(err)
		}
	}

	// adding node with existing addr should fail
	for _, n := range ldr.Info().Configs().Latest.Nodes {
		if err := waitAddNonvoter(ldr, 12, n.Addr, false); err == nil {
			t.Fatal(err)
		}
	}

	// ensure that config is not changed because of above errors
	if configsNow := ldr.Info().Configs(); !reflect.DeepEqual(configsNow, configs) {
		t.Log("old: ", configs)
		t.Log("new: ", configsNow)
		t.Fatal("configs changed")
	}
}

func test_changeConfig_committedByAll(t *testing.T) {
	// launch 2 node cluster M1, M2
	c, ldr, followers := launchCluster(t, 2)
	defer c.shutdown()

	configRelated := c.registerFor(configRelated)
	defer c.unregister(configRelated)

	// add M3 as nonvoter, wait for success reply
	c.ensure(waitAddNonvoter(ldr, 3, id2Addr(3), false))

	// ensure that leader raised configChange
	select {
	case e := <-configRelated.ch:
		if e.src != ldr.NID() {
			t.Fatalf("got M%d, want M%d", e.src, ldr.NID())
		}
		if e.typ != configChanged {
			t.Fatalf("got %d, want %d", e.typ, configChanged)
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
		if e.typ != configChanged {
			t.Fatalf("got %d, want %d", e.typ, configChanged)
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
		if e.typ != configCommitted {
			t.Fatalf("got %d, want %d", e.typ, configCommitted)
		}
	default:
		t.Fatal("expected configCommitted from ldr")
	}

	// wait and ensure that followers raised configCommitted
	c.waitForCommitted(ldr.Info().LastLogIndex(), followers[0])
	select {
	case e := <-configRelated.ch:
		if e.src != followers[0].NID() {
			t.Fatalf("got M%d, want M%d", e.src, followers[0].NID())
		}
		if e.typ != configCommitted {
			t.Fatalf("got %d, want %d", e.typ, configCommitted)
		}
	default:
		t.Fatal("expected configCommit from follower")
	}

	// launch new raft instance M3, without bootstrap
	m3 := c.launch(1, false)[3]

	// ensure that config committed by all
	c.waitForCommitted(ldr.Info().LastLogIndex(), m3)
	for _, r := range c.rr {
		info := r.Info()
		if !info.Configs().IsCommitted() {
			t.Fatalf("config is not committed by M%d %s", info.NID(), info.State())
		}
		m3, ok := info.Configs().Committed.Nodes[3]
		if !ok {
			t.Fatalf("m3 is not present in M%d %s", info.NID(), info.State())
		}
		if m3.Voter {
			t.Fatalf("m3 must be nonvoter in M%d %s", info.NID(), info.State())
		}
	}
}

// ------------------------------------------------------------------

func test_nonvoter_catchesUp_followsLeader(t *testing.T) {
	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// send 10 fsm updates, and wait for them to replicate
	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(10)

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)[4]

	// add M4 as nonvoter, wait for success reply
	c.ensure(waitAddNonvoter(ldr, m4.NID(), id2Addr(m4.NID()), false))

	// ensure that M4 got its FSM replicated
	c.waitFSMLen(10, m4)

	// send 10 more fsm updates, and wait for them to replicate
	c.sendUpdates(ldr, 11, 20)
	c.waitFSMLen(20)
}

func test_nonvoter_reconnects_catchesUp(t *testing.T) {
	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)[4]

	// add M4 as nonvoter, wait for success reply
	c.ensure(waitAddNonvoter(ldr, m4.NID(), id2Addr(m4.NID()), false))

	// now disconnect nonvoter m4
	m4StateChanged := c.registerFor(stateChanged, m4)
	defer c.unregister(m4StateChanged)
	c.disconnect(m4)

	// ensure that m4 remains as follower and does not become candidate
	select {
	case e := <-m4StateChanged.ch:
		t.Fatalf("m4 changed state to %s", e.state)
	case <-time.After(5 * c.heartbeatTimeout):
	}

	// ensure that leader detected that m4 is unreachable
	c.waitUnreachableDetected(ldr, m4)

	// send 10 fsm updates, and wait for them to replicate to m1, m2, m3
	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(10, c.exclude(m4)...)

	// ensure that m4 did not get last 10 fsm updates
	c.ensureFSMLen(0, m4)

	// now reconnect m4
	c.connect()

	// wait and ensure that m4 got last 10 entries from new leader
	c.waitFSMLen(10, m4)

	// restart m4, and check that he started with earlier config
	before := m4.Info().Configs()
	m4 = c.restart(m4)
	after := m4.Info().Configs()
	if !reflect.DeepEqual(before.Latest, after.Latest) {
		t.Log("before.latest:", before.Latest)
		t.Log(" after.latest:", after.Latest)
		t.Fatal("latest config after restart did not match")
	}
}

func test_nonvoter_leaderChanged_followsNewLeader(t *testing.T) {
	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)[4]

	// add M4 as nonvoter, wait for success reply
	c.ensure(waitAddNonvoter(ldr, m4.NID(), id2Addr(m4.NID()), false))

	// now shutdown the leader
	<-ldr.Shutdown()

	// wait for newLeader
	newLdr := c.waitForLeader(c.exclude(ldr)...)

	// send 10 fsm updates to new leader, and wait for them to replicate to all
	c.sendUpdates(newLdr, 1, 10)
	c.waitFSMLen(10, c.exclude(ldr)...)
	c.ensureFSMSame(nil, c.exclude(ldr)...)
}
