package raft

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestRaft_AddNode(t *testing.T) {
	Debug("\nTestRaft_AddNode --------------------------")
	defer leaktest.Check(t)()

	// launch 3 node cluster M1, M2, M3
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()
	c.ensureLeader(ldr.ID())

	configs := ldr.Info().Configs()

	// adding node with empty id should fail
	n := Node{Addr: "localhost:8888", Voter: false}
	if _, err := waitTask(ldr, AddNonvoter(n), 0); err == nil {
		t.Fatal(err)
	}

	// adding node with empty addr should fail
	n = Node{ID: ID("M10"), Voter: false}
	if _, err := waitTask(ldr, AddNonvoter(n), 0); err == nil {
		t.Fatal(err)
	}

	// adding voter should fail
	n = Node{ID: ID("M11"), Addr: "M10:8888", Voter: true}
	if _, err := waitTask(ldr, AddNonvoter(n), 0); err == nil {
		t.Fatal(err)
	}

	// adding node with existing id should fail
	for _, n := range ldr.Info().Configs().Latest.Nodes {
		n := Node{ID: n.ID, Addr: "localhost:8888", Voter: false}
		if _, err := waitTask(ldr, AddNonvoter(n), 0); err == nil {
			t.Fatal(err)
		}
	}

	// adding node with existing addr should fail
	for _, n := range ldr.Info().Configs().Latest.Nodes {
		n := Node{ID: "M12", Addr: n.Addr, Voter: false}
		if _, err := waitTask(ldr, AddNonvoter(n), 0); err == nil {
			t.Fatal(err)
		}
	}

	// ensure that config is not changed because of above errors
	if configsNow := ldr.Info().Configs(); !reflect.DeepEqual(configsNow, configs) {
		t.Log("old: ", configs)
		t.Log("new: ", configsNow)
		t.Fatal("configs changed")
	}

	// send 10 fsm updates, and wait for them to replicate
	for i := 0; i < 10; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("msg-%d", i)))
	}
	c.waitFSMLen(10)

	// launch new raft instance M4, without bootstrap
	c.launch(1, false)
	m4 := c.rr["M4"]

	configRelated := c.registerForEvent(configRelated, c.exclude(m4)...)
	defer c.unregisterObserver(configRelated)

	// add M4 as nonvoter, wait for success reply
	task := AddNonvoter(Node{ID: m4.ID(), Addr: "M4:8888", Voter: false})
	ldr.Tasks() <- task
	<-task.Done()
	if task.Err() != nil {
		t.Fatal(task.Err())
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
	limit := time.After(2 * c.heartbeatTimeout)
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
		case <-limit:
			t.Fatal("expected configCommit from follower")
		}
	}

	// ensure that leader has now config committed
	if !ldr.Info().Configs().IsCommitted() {
		t.Fatal("config is not committed")
	}

	// ensure that M4 got its FSM replicated
	c.waitFSMLen(10, m4)

	// send 10 fsm updates, and wait for them to replicate
	for i := 10; i < 20; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("msg-%d", i)))
	}
	c.waitFSMLen(20)

	// now disconnect nonvoter m4
	unreachable := c.registerForEvent(unreachable, ldr)
	defer c.unregisterObserver(unreachable)
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
	select {
	case e := <-unreachable.ch:
		if e.target != m4.ID() {
			t.Fatalf("leader.unreachable: got %s, want m4", e.target)
		}
	case <-time.After(c.longTimeout):
		t.Fatal("leader could not detect that m4 got disconnected")
	}

	// send 10 fsm updates, and wait for them to replicate to m1, m2, m3
	for i := 20; i < 30; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("msg-%d", i)))
	}
	c.waitFSMLen(30, c.exclude(m4)...)

	// ensure that m4 did not get last 10 fsm updates
	if got := fsm(m4).len(); got != 20 {
		t.Fatalf("m4.fsmLen: got %d, want 20", got)
	}

	// now shutdown the leader
	ldr.Shutdown().Wait()

	// wait for newLeader
	c.waitForLeader(c.longTimeout, c.exclude(ldr)...)

	// now reconnect m4
	c.connect()

	// wait and ensure that m4 got last 10 entries from new leader
	c.waitFSMLen(30, m4)

	// restart m4, and check that he started with earlier config
	before := m4.Info().Configs()
	m4 = c.restart(m4)
	after := m4.Info().Configs()
	if !reflect.DeepEqual(before.Latest, after.Latest) {
		t.Log("before.latest:", before.Latest)
		t.Log(" after.latest:", after.Latest)
		t.Fatal("latest config after restart did not match")
	}

	// ensure that m4's fsm restored after restart
	c.waitFSMLen(30, m4)

	after = m4.Info().Configs()
	if !reflect.DeepEqual(before, after) {
		t.Log("before:", before)
		t.Log(" after:", after)
		t.Fatal("configs after restart did not match")
	}

	// ensure that his config is committed
	if !m4.Info().Configs().IsCommitted() {
		t.Fatal("m4 configs should have been committed")
	}

}
