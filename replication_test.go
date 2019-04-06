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

func TestReplication_behindFollower(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// disconnect one follower
	behind := c.followers()[0]
	c.disconnect(behind)

	// wait until leader detects that follower is unreachable
	c.waitUnreachableDetected(ldr, behind)

	// commit a lot of things
	<-c.sendUpdates(ldr, 1, 10).Done()

	// reconnect the behind node
	c.connect()

	// ensure all the logs are the same
	c.waitFSMLen(10)
	c.ensureFSMSame(nil)

	// Ensure one leader
	c.ensureLeader(c.leader().NID())
}

func TestReplication_nonvoter_catchesUp_followsLeader(t *testing.T) {
	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// send 10 fsm updates, and wait for them to replicate
	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(10)

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)[4]

	// add M4 as nonvoter, wait for success reply
	c.ensure(waitAddNonvoter(ldr, m4.NID(), c.id2Addr(m4.NID()), false))

	// ensure that M4 got its FSM replicated
	c.waitFSMLen(10, m4)

	// send 10 more fsm updates, and wait for them to replicate
	c.sendUpdates(ldr, 11, 20)
	c.waitFSMLen(20)
}

func TestReplication_nonvoter_reconnects_catchesUp(t *testing.T) {
	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)[4]

	// now disconnect nonvoter m4
	m4StateChanged := c.registerFor(stateChanged, m4)
	defer c.unregister(m4StateChanged)
	c.disconnect(m4)

	// ensure that m4 remains as follower and does not become candidate
	select {
	case e := <-m4StateChanged.ch:
		t.Fatalf("m4 changed state to %s", e.state)
	case <-time.After(5 * c.heartbeatTimeout): // todo: can we reduce time ??
	}

	// wait until leader is commit ready
	c.waitCommitReady(ldr)

	// add M4 as nonvoter, wait for success reply
	c.ensure(waitAddNonvoter(ldr, m4.NID(), c.id2Addr(m4.NID()), false))

	// ensure that leader detected that m4 is unreachable
	c.waitUnreachableDetected(ldr, m4)

	// send 10 fsm updates, and wait for them to replicate to m1, m2, m3
	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(10, c.exclude(m4)...)

	// ensure that m4 did not get last 10 fsm updates
	c.ensureFSMLen(0, m4)

	// now reconnect m4
	c.connect()

	// ensure that leader detected that m4 is reachable
	c.waitReachableDetected(ldr, m4)

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

func TestReplication_nonvoter_leaderChanged_followsNewLeader(t *testing.T) {
	// launch 3 node cluster M1, M2, M3
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// launch new raft instance M4, without bootstrap
	m4 := c.launch(1, false)[4]

	// wait until leader is commit ready
	c.waitCommitReady(ldr)

	// add M4 as nonvoter, wait for success reply
	c.ensure(waitAddNonvoter(ldr, m4.NID(), c.id2Addr(m4.NID()), false))

	// now shutdown the leader
	c.shutdown(ldr)

	// wait for newLeader
	newLdr := c.waitForLeader(c.exclude(ldr)...)

	// send 10 fsm updates to new leader, and wait for them to replicate to all
	c.sendUpdates(newLdr, 1, 10)
	c.waitFSMLen(10, c.exclude(ldr)...)
	c.ensureFSMSame(nil, c.exclude(ldr)...)
}

func TestReplication_installSnap(t *testing.T) {
	t.Run("case1", func(t *testing.T) {
		testInstallSnapCase(t, false)
	})
	t.Run("case2", func(t *testing.T) {
		testInstallSnapCase(t, true)
	})
}

func testInstallSnapCase(t *testing.T, updateFSMAfterSnap bool) {
	// launch 3 node cluster
	c := newCluster(t)
	c.storeOpt.LogSegmentSize = 1024
	ldr, _ := c.ensureLaunch(3)
	defer c.shutdown()

	// send 100 updates, wait for them
	updates := uint64(30)
	<-c.sendUpdates(ldr, 1, 30).Done()

	// add two nonVoters M4; wait all commit them
	c.ensure(waitAddNonvoter(ldr, 4, c.id2Addr(4), false))
	c.waitCatchup()

	logCompacted := c.registerFor(logCompacted, ldr)
	defer c.unregister(logCompacted)

	// take snapshot
	c.takeSnapshot(ldr, 1, nil)

	// ensure log compacted
	c.ensure(logCompacted.waitForEvent(c.longTimeout))

	if updateFSMAfterSnap {
		// send 10 more updates, ensure all alive get them
		updates += 10
		c.sendUpdates(ldr, 1, 10)
		c.waitFSMLen(updates)
	}

	// now launch nonVoter M4; ensure he catches up
	m4 := c.launch(1, false)[4]
	c.waitFSMLen(updates, m4)

	// send 10 more updates, ensure all alive get them
	updates += 10
	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(updates)
}
