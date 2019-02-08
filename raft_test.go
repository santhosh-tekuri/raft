package raft

import (
	"testing"
	"time"

	"github.com/santhosh-tekuri/raft/inmem"
)

func TestRaft(t *testing.T) {
	addrs := freeAddrs(3)
	t.Log("cluster", addrs)

	servers := make([]*Raft, len(addrs))
	for i := range servers {
		members := make([]string, len(servers))
		copy(members, addrs)
		members[0], members[i] = members[i], members[0]
		t.Log("members", members)
		storage := new(inmem.Storage)
		servers[i] = New(members, storage, storage)
		if err := servers[i].Listen(); err != nil {
			t.Fatalf("raft.listen failed: %v", err)
		}
		go servers[i].Serve()
		// todo: defer server shutdown
	}

	t.Log("sleeping....")
	time.Sleep(30 * time.Second)
}
