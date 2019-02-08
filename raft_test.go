package raft

import (
	"testing"
	"time"

	"github.com/santhosh-tekuri/raft/inmem"
)

func TestRaft(t *testing.T) {
	addrs := freeAddrs(3)
	t.Log("cluster", addrs)

	rr := make([]*Raft, len(addrs))
	for i := range rr {
		members := make([]string, len(rr))
		copy(members, addrs)
		members[0], members[i] = members[i], members[0]
		t.Log("members", members)
		storage := new(inmem.Storage)
		rr[i] = New(members, storage, storage)
		if err := rr[i].Listen(); err != nil {
			t.Fatalf("raft.listen failed: %v", err)
		}
		go rr[i].Serve()
		// todo: defer server shutdown
	}

	t.Log("sleeping....")
	time.Sleep(10 * time.Second)

	for _, r := range rr {
		if r.getState() == leader {
			debug(r, "request apply cmd")
			r.Apply([]byte("how are you?"), nil)
		}
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
