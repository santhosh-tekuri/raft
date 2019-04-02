package raft

import (
	"reflect"
	"testing"
)

func TestLdrShip_changeConfig_validations(t *testing.T) {
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
		if err := config.AddNonvoter(id, "localhost:8888", false); err == nil {
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

func TestLdrShip_changeConfig_trace(t *testing.T) {
	// launch 2 node cluster M1, M2
	c, ldr, followers := launchCluster(t, 2)
	defer c.shutdown()

	// wait for bootstrap config committed by all
	c.waitForCommitted(ldr.Info().LastLogIndex())

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
