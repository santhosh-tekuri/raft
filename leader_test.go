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
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLeader_stepDown(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.longTimeout)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if resp.msg != "test" {
		t.Fatalf("apply response mismatch. got %s, want test", resp.msg)
	}
	if resp.index != 1 {
		t.Fatalf("fsmReplyIndex: got %d want 1", resp.index)
	}
	c.waitFSMLen(1)

	// disconnect leader
	ldrTerm := ldr.Info().Term()
	c.disconnect(ldr)

	// leader should stepDown
	c.waitForState(ldr, c.longTimeout, Follower, Candidate)

	// wait for new leader
	newLdr := c.waitForLeader(c.exclude(ldr)...)

	// ensure leader term is greater
	if newLdrTerm := newLdr.Info().Term(); newLdrTerm <= ldrTerm {
		t.Fatalf("expected new leader term: newLdrTerm=%d, ldrTerm=%d", newLdrTerm, ldrTerm)
	}

	// apply should not work on old leader
	_, err = waitUpdate(ldr, "reject", c.longTimeout)
	if err, ok := err.(NotLeaderError); !ok {
		t.Fatalf("got %v, want NotLeaderError", err)
	} else if err.Leader.Addr != "" {
		t.Fatalf("got %s, want ", err.Leader.Addr)
	}

	// apply should work on new leader
	if _, err = waitUpdate(newLdr, "accept", c.longTimeout); err != nil {
		t.Fatalf("got %v, want nil", err)
	}

	// reconnect the networks
	c.connect()
	c.waitForHealthy()

	// wait for log replication
	c.waitFSMLen(2)

	// Check two entries are applied to the FSM
	c.ensureFSMSame([]string{"test", "accept"})
}

func TestLeader_quorumWait_unreachable(t *testing.T) {
	c := newCluster(t)
	c.opt.QuorumWait = 2 * time.Second
	ldr, followers := c.ensureLaunch(2)
	defer c.shutdown()

	reachability := c.registerFor(eventUnreachable, ldr)
	defer c.unregister(reachability)
	quorumUnreachable := c.registerFor(eventQuorumUnreachable, ldr)
	defer c.unregister(quorumUnreachable)

	// disconnect the follower now
	c.disconnect(followers[0])

	// wait for leader to detect
	c.ensure(reachability.waitForEvent(c.heartbeatTimeout))
	start := time.Now()

	// check that we got quorumUnreachable trace
	e, err := quorumUnreachable.waitForEvent(c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("waitQuorumUnreachable: %v", err)
	}
	if e.since.IsZero() {
		t.Fatal("quorum must be unreachable")
	}
	if d := time.Now().Sub(start); d > 10*time.Millisecond {
		t.Fatalf("quorumUnreachable detection took %s", d)
	}
	start = time.Now()

	// wait for leader to stepDown
	c.waitForState(ldr, c.longTimeout, Follower, Candidate)

	// ensure that leader waited for quorumConfigured before stepDown
	if got := time.Now().Sub(start); got < 1*time.Second {
		t.Fatalf("quorumWait: got %s, want %s", got, c.opt.QuorumWait)
	}

	// should be no leaders
	if n := len(c.getInState(Leader)); n != 0 {
		t.Fatalf("#leaders: got %d, want 0", n)
	}

	// Ensure both have cleared their leader
	c.waitForState(followers[0], 2*c.heartbeatTimeout, Candidate)
	c.ensureLeader(0)
}

func TestLeader_quorumWait_reachable(t *testing.T) {
	c := newCluster(t)
	c.opt.QuorumWait = 30 * time.Minute
	ldr, followers := c.ensureLaunch(2)
	defer c.shutdown()

	reachability := c.registerFor(eventUnreachable, ldr)
	defer c.unregister(reachability)
	quorumUnreachable := c.registerFor(eventQuorumUnreachable, ldr)
	defer c.unregister(quorumUnreachable)

	// disconnect the follower now
	c.disconnect(followers[0])

	// wait for leader to detect
	c.ensure(reachability.waitForEvent(c.heartbeatTimeout))
	start := time.Now()

	// check that we got quorumUnreachable trace
	e, err := quorumUnreachable.waitForEvent(c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("waitQuorumUnreachable: %v", err)
	}
	if e.since.IsZero() {
		t.Fatal("quorum must be unreachable")
	}
	if d := time.Now().Sub(start); d > 10*time.Millisecond {
		t.Fatalf("quorumUnreachable detection took %s", d)
	}

	// connect the follower now
	c.connect()

	// check that we got quorumReachable trace
	e, err = quorumUnreachable.waitForEvent(c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("waitQuorumReachable: %v", err)
	}
	if !e.since.IsZero() {
		t.Fatal("quorum must be reachable")
	}
}

func TestLeader_updateFSM_nonLeader(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// apply should work not work on non-leader
	ldrAddr := ldr.Info().Addr()
	for _, r := range c.rr {
		if r != ldr {
			_, err := waitUpdate(r, "reject", c.longTimeout)
			if err, ok := err.(NotLeaderError); !ok {
				t.Fatalf("got %v, want NotLeaderError", err)
			} else if err.Leader.Addr != ldrAddr {
				t.Fatalf("got %s, want %s", err.Leader.Addr, ldrAddr)
			}
		}
	}
}

func TestLeader_updateFSM_concurrent(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// concurrently apply
	var wg sync.WaitGroup
	n := uint64(100)
	for i := uint64(0); i < n; i++ {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			if _, err := waitUpdate(ldr, fmt.Sprintf("test%d", i), 0); err != nil {
				t.Log("FAIL got", err, "want nil")
				t.Fail() // note: t.Fatal should note be called from non-test goroutine
			}
		}(i)
	}

	// wait to finish
	c.ensure(waitWG(&wg, c.longTimeout))

	// check If anything failed
	if t.Failed() {
		t.Fatal("one or more of the apply operations failed")
	}

	// check the FSMs
	c.waitFSMLen(n)
	c.ensureFSMSame(nil)
}

func TODO_TestLeader_backPressure(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	stateChanged := c.registerFor(eventStateChanged, ldr)
	defer c.unregister(stateChanged)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.sendUpdates(ldr, 1, 100000)
		}()
	}

	wgCh := wgChannel(&wg)
	timer := time.Tick(15 * time.Second)
	for {
		select {
		case <-wgCh:
			fmt.Println("waitgroup finished")
			info := ldr.Info()
			fmt.Println("lastLogIndex:", info.LastLogIndex(), "committed:", info.Committed())
			return
		case e := <-stateChanged.ch:
			fmt.Println("statechanged:", e.state)
			info := ldr.Info()
			fmt.Println("lastLogIndex:", info.LastLogIndex(), "committed:", info.Committed())
			t.Fatalf("leader changed state to %s", e.state)
		case <-timer:
			info := ldr.Info()
			fmt.Println("lastLogIndex:", info.LastLogIndex(), "committed:", info.Committed())
		}
	}
}

func TestLeader_barrierFSM(t *testing.T) {
	c, ldr, followers := launchCluster(t, 3)
	defer c.shutdown()

	// commit a lot of things and wait for barrier
	c.sendUpdates(ldr, 1, 100)
	c.waitBarrier(ldr, 0)

	// ensure leader fsm got all commands
	c.ensureFSMLen(100, ldr)

	// ensure leader's lastLogIndex matches with at-least one of follower
	len0 := ldr.Info().LastLogIndex()
	len1 := followers[0].Info().LastLogIndex()
	len2 := followers[1].Info().LastLogIndex()
	if len0 != len1 && len0 != len2 {
		t.Fatalf("len0 %d, len1 %d, len2 %d", len0, len1, len2)
	}

	// ensure that barrier is not stored in log
	want := ldr.Info().LastLogIndex()
	c.waitBarrier(ldr, 0)
	if got := ldr.Info().LastLogIndex(); got != want {
		t.Fatalf("lastLogIndex: got %d, want %d", got, want)
	}
}

func TestLeader_readFSM(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// wait for fsm ready
	c.waitBarrier(ldr, 0)

	// send query
	want := ldr.Info().LastLogIndex()
	if _, err := waitRead(ldr, "last", 0); err != errNoCommands {
		t.Fatalf("got %v, want %v", err, errNoCommands)
	}

	// ensure query is not stored in log
	if got := ldr.Info().LastLogIndex(); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}

	// ensure fsm is not changed
	if got := fsm(ldr).len(); got != 0 {
		t.Fatalf("got %d, want %d", got, 0)
	}

	// send updates, in between do queries and check query reply
	for i := 0; i < 101; i++ {
		cmd := fmt.Sprintf("cmd%d", i)
		ldr.FSMTasks() <- UpdateFSM([]byte(cmd))
		if i%10 == 0 {
			qq := []FSMTask{
				ReadFSM("last"),
				ReadFSM("last"),
			}
			for _, q := range qq {
				ldr.FSMTasks() <- q
			}
			for _, q := range qq {
				<-q.Done()
				if q.Err() != nil {
					t.Fatal(q.Err())
				}
				reply := fsmReply{cmd, i}
				if q.Result() != reply {
					t.Fatalf("got %v, want %v", q.Result(), reply)
				}
			}
		}
	}

	// ensure queries are not stored in log
	want += 101
	if got := ldr.Info().LastLogIndex(); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}

	// ensure fsm has all commands but not queries
	c.ensureFSMLen(101, ldr)
}
