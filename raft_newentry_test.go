package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestRaft_ApplyNonLeader(t *testing.T) {
	Debug("\nTestRaft_ApplyNonLeader --------------------------")
	defer leaktest.Check(t)()
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// apply should work not work on non-leader
	ldrAddr := ldr.Info().Addr()
	for _, r := range c.rr {
		if r != ldr {
			_, err := waitUpdate(r, "reject", c.commitTimeout)
			if err, ok := err.(NotLeaderError); !ok {
				t.Fatalf("got %v, want NotLeaderError", err)
			} else if err.Leader != ldrAddr {
				t.Fatalf("got %s, want %s", err.Leader, ldrAddr)
			}
		}
	}
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	Debug("\nTestRaft_ApplyConcurrent --------------------------")
	defer leaktest.Check(t)()
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// concurrently apply
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := waitUpdate(ldr, fmt.Sprintf("test%d", i), 0); err != nil {
				Debug("FAIL got", err, "want nil")
				t.Fail() // note: t.Fatal should note be called from non-test goroutine
			}
		}(i)
	}

	// wait to finish
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		break
	case <-time.After(c.longTimeout):
		t.Fatal("timeout")
	}

	// check If anything failed
	if t.Failed() {
		t.Fatal("one or more of the apply operations failed")
	}

	// check the FSMs
	c.waitFSMLen(100)
	c.ensureFSMSame(nil)
}

func TestRaft_Barrier(t *testing.T) {
	Debug("\nTestRaft_Barrier --------------------------")
	defer leaktest.Check(t)()
	c, ldr, followers := launchCluster(t, 3)
	defer c.shutdown()

	// commit a lot of things
	n := 100
	for i := 0; i < n; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("test%d", i)))
	}

	// wait for a barrier complete
	if err := waitBarrier(ldr, 0); err != nil {
		t.Fatalf("barrier failed: %v", err)
	}

	// ensure leader fsm got all commands
	if got := fsm(ldr).len(); int(got) != n {
		t.Fatalf("#entries: got %d, want %d", got, n)
	}

	// ensure leader's lastLogIndex matches with at-least one of follower
	len0 := ldr.Info().LastLogIndex()
	len1 := followers[0].Info().LastLogIndex()
	len2 := followers[1].Info().LastLogIndex()
	if len0 != len1 && len0 != len2 {
		t.Fatalf("len0 %d, len1 %d, len2 %d", len0, len1, len2)
	}

	// ensure that barrier is not stored in log
	want := ldr.Info().LastLogIndex()
	if err := waitBarrier(ldr, 0); err != nil {
		t.Fatalf("barrier failed: %v", err)
	}
	if got := ldr.Info().LastLogIndex(); got != want {
		t.Fatalf("lastLogIndex: got %d, want %d", got, want)
	}
}

func TestRaft_Query(t *testing.T) {
	Debug("\nTestRaft_Query --------------------------")
	defer leaktest.Check(t)()
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// wait for fsm ready
	if err := waitBarrier(ldr, 0); err != nil {
		t.Fatalf("barrier failed: %v", err)
	}

	// send query
	want := ldr.Info().LastLogIndex()
	if _, err := waitQuery(ldr, "query:last", 0); err != errNoCommands {
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
		ldr.NewEntries() <- UpdateEntry([]byte(cmd))
		if i%10 == 0 {
			qq := []NewEntry{
				QueryEntry([]byte("query:last")),
				QueryEntry([]byte("query:last")),
			}
			for _, q := range qq {
				ldr.NewEntries() <- q
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
	if got := fsm(ldr).len(); got != 101 {
		t.Fatalf("got %d, want %d", got, 100)
	}
}
