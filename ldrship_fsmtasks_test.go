package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLdrShip_fsmTasks_update_nonLeader(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// apply should work not work on non-leader
	ldrAddr := ldr.Info().Addr()
	for _, r := range c.rr {
		if r != ldr {
			_, err := waitUpdate(r, "reject", c.longTimeout)
			if err, ok := err.(NotLeaderError); !ok {
				t.Fatalf("got %v, want NotLeaderError", err)
			} else if err.LeaderAddr != ldrAddr {
				t.Fatalf("got %s, want %s", err.LeaderAddr, ldrAddr)
			}
		}
	}
}

func TestLdrShip_fsmTasks_update_concurrent(t *testing.T) {
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

func TODO_TestLdrShip_fsmTasks_backPressure(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	stateChanged := c.registerFor(stateChanged, ldr)
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

func TestLdrShip_fsmTasks_barrier(t *testing.T) {
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

func TestLdrShip_fsmTasks_read(t *testing.T) {
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
				ReadFSM([]byte("last")),
				ReadFSM([]byte("last")),
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
