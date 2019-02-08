package raft

import (
	"testing"
	"time"

	"github.com/santhosh-tekuri/raft/inmem"
)

func TestRaft(t *testing.T) {
	addrs := freeAddrs(3)

	leaderCh := make(chan *Raft, 1)
	stateChanged = func(r *Raft) {
		if r.state == leader {
			select {
			case leaderCh <- r:
			default:
			}
		}
	}
	defer func() { stateChanged = func(*Raft) {} }()

	rr := make([]*Raft, len(addrs))
	for i := range rr {
		members := make([]string, len(rr))
		copy(members, addrs)
		members[0], members[i] = members[i], members[0]
		storage := new(inmem.Storage)
		rr[i] = New(members, storage, storage)
		if err := rr[i].Listen(); err != nil {
			t.Fatalf("raft.listen failed: %v", err)
		}
		go rr[i].Serve()
		// todo: defer server shutdown
	}

	select {
	case r := <-leaderCh:
		if r.getState() != leader {
			t.Fatal("leader lost leadership")
		}
		debug(r, "request apply cmd")
		r.Apply([]byte("how are you?"), nil)
	case <-time.After(10 * time.Second):
		t.Fatal("no leader even after 10 sec")
	}

	time.Sleep(10 * time.Second)
}

// ---------------------------------------------

func (r *Raft) getState() state {
	var s state
	r.inspect(func(r *Raft) {
		s = r.state
	})
	return s
}
