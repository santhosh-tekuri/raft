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
	"testing"
	"time"
)

func TestRaft_bootstrap(t *testing.T) {
	c := newCluster(t)

	electionAborted := c.registerFor(eventElectionAborted)
	defer c.unregister(electionAborted)

	// launch cluster without bootstrapping
	c.launch(3, false)
	defer c.shutdown()

	// all nodes should must abort election and only once
	testln("waitForElectionAborted")
	timeout := time.After(c.longTimeout)
	aborted := make(map[uint64]bool)
	for i := 0; i < 3; i++ {
		select {
		case e := <-electionAborted.ch:
			if aborted[e.src] {
				t.Fatalf("aborted twice")
			}
			aborted[e.src] = true
		case <-timeout:
			t.Fatal("timout in waiting for abort election")
		}
	}

	// bootstrap one of the nodes
	testln("prepareBootStrapConfig")
	ldr := c.rr[1]
	config := c.info(ldr).Configs.Latest
	for _, r := range c.rr {
		if err := config.AddVoter(r.NID(), c.id2Addr(r.NID())); err != nil {
			c.Fatal(err)
		}
	}
	if err := waitBootstrap(ldr, config, c.longTimeout); err != nil {
		t.Fatal(err)
	}

	// the bootstrapped node should be the leader
	c.waitForLeader(ldr)
	c.waitForFollowers()

	// should be able to update
	if _, err := waitUpdate(ldr, "hello", 0); err != nil {
		t.Fatal(err)
	}
	c.waitFSMLen(1)
	c.ensureFSMSame([]string{"hello"})

	// ensure bootstrap fails if already bootstrapped
	if err := waitBootstrap(ldr, config, c.longTimeout); err != ErrStaleConfig {
		t.Fatalf("got %v, want %v", err, ErrStaleConfig)
	}
	err := waitBootstrap(c.rr[2], config, c.longTimeout)
	if _, ok := err.(NotLeaderError); !ok {
		t.Fatalf("got %v, want NotLeaderError", err)
	}

	// disconnect leader, and ensure that new leader is chosen
	c.disconnect(ldr)
	c.waitForLeader(c.exclude(ldr)...)
}

func TestRaft_bootstrap_validations(t *testing.T) {
	c := newCluster(t)

	// launch cluster without bootstrapping
	c.launch(3, false)
	defer c.shutdown()

	ldr := c.rr[1]
	validConfig := c.info(ldr).Configs.Latest
	for _, r := range c.rr {
		if err := validConfig.AddVoter(r.NID(), c.id2Addr(r.NID())); err != nil {
			c.Fatal(err)
		}
	}

	// bootstrap with empty address
	config := validConfig.clone()
	self := config.Nodes[ldr.nid]
	self.Addr = ""
	config.Nodes[ldr.nid] = self
	if err := waitBootstrap(ldr, config, c.longTimeout); err == nil {
		t.Fatal("error expected")
	}

	// bootstrap without self
	config = validConfig.clone()
	delete(config.Nodes, ldr.nid)
	if err := waitBootstrap(ldr, config, c.longTimeout); err == nil {
		t.Fatal("error expected")
	}

	// bootstrap self as nonVoter
	config = validConfig.clone()
	self = config.Nodes[ldr.nid]
	self.Voter = false
	config.Nodes[ldr.nid] = self
	if err := waitBootstrap(ldr, config, c.longTimeout); err == nil {
		t.Fatal("error expected")
	}

	// bootstrap with unstable config
	config = validConfig.clone()
	self = config.Nodes[ldr.nid]
	self.Action = Demote
	config.Nodes[ldr.nid] = self
	if err := waitBootstrap(ldr, config, c.longTimeout); err == nil {
		t.Fatal("error expected")
	}
}
