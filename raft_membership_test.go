package raft

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestRaft_AddNonVoter_validations(t *testing.T) {
	Debug("\nTestRaft_AddNonVoter_validations --------------------------")
	defer leaktest.Check(t)()
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	configs := ldr.Info().Configs()

	// adding node with empty id should fail
	if _, err := waitTask(ldr, AddNonvoter("", "localhost:8888", false), 0); err == nil {
		t.Fatal(err)
	}

	// adding node with empty addr should fail
	if _, err := waitTask(ldr, AddNonvoter("M10", "", false), 0); err == nil {
		t.Fatal(err)
	}

	// adding node with existing id should fail
	for _, n := range ldr.Info().Configs().Latest.Nodes {
		if _, err := waitTask(ldr, AddNonvoter(n.ID, "localhost:8888", false), 0); err == nil {
			t.Fatal(err)
		}
	}

	// adding node with existing addr should fail
	for _, n := range ldr.Info().Configs().Latest.Nodes {
		if _, err := waitTask(ldr, AddNonvoter("M12", n.Addr, false), 0); err == nil {
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

func TestRaft_AddNonVoter_committedByAll(t *testing.T) {
	Debug("\nTestRaft_AddNonVoter_committedByAll --------------------------")
	defer leaktest.Check(t)()
	// launch 3 node cluster M1, M2, M3
	c, ldr, followers := launchCluster(t, 3)
	defer c.shutdown()

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)["M4"]

	configRelated := c.registerForEvent(configRelated, c.exclude(m4)...)
	defer c.unregisterObserver(configRelated)

	// add M4 as nonvoter, wait for success reply
	if _, err := waitTask(ldr, AddNonvoter(m4.ID(), "M4:8888", false), 0); err != nil {
		t.Fatal(err)
	}

	// ensure that leader raised configChange
	select {
	case e := <-configRelated.ch:
		if e.src != ldr.ID() {
			t.Fatalf("got %s, want %s", e.src, ldr.ID())
		}
		if e.typ != configChanged {
			t.Fatalf("got %d, want %d", e.typ, configChanged)
		}
	default:
		t.Fatal("expected configChange from ldr")
	}

	// ensure that followers raised configChange, exactly once
	set := make(map[ID]bool)
	for i := 0; i < 2; i++ {
		select {
		case e := <-configRelated.ch:
			if e.src == ldr.ID() || e.src == m4.ID() {
				t.Fatalf("got %s", e.src)
			}
			if e.typ != configChanged {
				t.Fatalf("got %d, want %d", e.typ, configChanged)
			}
			if set[e.src] {
				t.Fatalf("duplicate configChange from %s", e.src)
			}
		default:
			t.Fatal("expected configChange from follower")
		}
	}

	// ensure that leader raised configCommitted
	select {
	case e := <-configRelated.ch:
		if e.src != ldr.ID() {
			t.Fatalf("got %s, want %s", e.src, ldr.ID())
		}
		if e.typ != configCommitted {
			t.Fatalf("got %d, want %d", e.typ, configCommitted)
		}
	default:
		t.Fatal("expected configCommitted from ldr")
	}

	// wait and ensure that followers raised configCommitted
	c.waitCatchup(followers[0])
	c.waitCatchup(followers[1])
	set = make(map[ID]bool)
	for i := 0; i < 2; i++ {
		select {
		case e := <-configRelated.ch:
			if e.src == ldr.ID() || e.src == m4.ID() {
				t.Fatalf("got %s", e.src)
			}
			if e.typ != configCommitted {
				t.Fatalf("got %d, want %d", e.typ, configCommitted)
			}
			if set[e.src] {
				t.Fatalf("duplicate configCommitted from %s", e.src)
			}
		default:
			t.Fatal("expected configCommit from follower")
		}
	}

	// ensure that config committed by all
	c.waitCatchup(m4)
	for _, r := range c.rr {
		info := r.Info()
		if !info.Configs().IsCommitted() {
			t.Fatalf("config is not committed by %s %s", info.ID(), info.State())
		}
		m4, ok := info.Configs().Committed.Nodes[ID("M4")]
		if !ok {
			t.Fatalf("m4 is not present in %s %s", info.ID(), info.State())
		}
		if m4.Voter {
			t.Fatalf("m4 must be nonvoter in %s %s", info.ID(), info.State())
		}
	}
}

func TestRaft_AddNonVoter_catchesUp_followsLeader(t *testing.T) {
	Debug("\nTestRaft_AddNonVoter_catchesUp_followsLeader --------------------------")
	defer leaktest.Check(t)()

	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// send 10 fsm updates, and wait for them to replicate
	for i := 0; i < 10; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("msg-%d", i)))
	}
	c.waitFSMLen(10)

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)["M4"]

	// add M4 as nonvoter, wait for success reply
	if _, err := waitTask(ldr, AddNonvoter(m4.ID(), "M4:8888", false), 0); err != nil {
		t.Fatal(err)
	}

	// ensure that M4 got its FSM replicated
	c.waitFSMLen(10, m4)

	// send 10 fsm updates, and wait for them to replicate
	for i := 10; i < 20; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("msg-%d", i)))
	}
	c.waitFSMLen(20)
}

func TestRaft_AddNonVoter_nonVoterReconnects_catchesUp(t *testing.T) {
	Debug("\nTestRaft_AddNonVoter_nonVoterReconnects_catchesUp --------------------------")
	defer leaktest.Check(t)()

	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)["M4"]

	// add M4 as nonvoter, wait for success reply
	if _, err := waitTask(ldr, AddNonvoter(m4.ID(), "M4:8888", false), 0); err != nil {
		t.Fatal(err)
	}

	// now disconnect nonvoter m4
	m4StateChanged := c.registerForEvent(stateChanged, m4)
	defer c.unregisterObserver(m4StateChanged)
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
	for i := 0; i < 10; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("msg-%d", i)))
	}
	c.waitFSMLen(10, c.exclude(m4)...)

	// ensure that m4 did not get last 10 fsm updates
	if got := fsm(m4).len(); got != 0 {
		t.Fatalf("m4.fsmLen: got %d, want 0", got)
	}

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

func TestRaft_AddNonVoter_leaderChanged_followsNewLeader(t *testing.T) {
	Debug("\nTestRaft_AddNonVoter_leaderChanged_followsNewLeader --------------------------")
	defer leaktest.Check(t)()

	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)["M4"]

	// add M4 as nonvoter, wait for success reply
	if _, err := waitTask(ldr, AddNonvoter(m4.ID(), "M4:8888", false), 0); err != nil {
		t.Fatal(err)
	}

	// now shutdown the leader
	ldr.Shutdown().Wait()

	// wait for newLeader
	newLdr := c.waitForLeader(c.longTimeout, c.exclude(ldr)...)

	// send 10 fsm updates to new leader, and wait for them to replicate to all
	for i := 0; i < 10; i++ {
		newLdr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("msg-%d", i)))
	}
	c.waitFSMLen(10, c.exclude(ldr)...)
	c.ensureFSMSame(nil, c.exclude(ldr)...)
}
