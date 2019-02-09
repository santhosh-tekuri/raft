package raft

import (
	"sync"
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
		rr[i] = New(members, &fsmMock{}, storage, storage)
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
		respCh := make(chan interface{}, 1)
		r.Apply([]byte("how are you?"), respCh)
		if resp := <-respCh; resp != "applied: how are you?" {
			t.Fatalf("reply mismatch. got %v, want %s", resp, "applied: how are you?")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no leader even after 10 sec")
	}

	time.Sleep(10 * time.Second)
	for _, r := range rr {
		if cmd := r.fsm.(*fsmMock).lastCommand(); string(cmd) != "how are you?" {
			t.Errorf("%s lastCommand. got %q, want %q", r.getState(), string(cmd), "how are you?")
		}
	}
}

// ---------------------------------------------

type fsmMock struct {
	mu   sync.RWMutex
	cmds [][]byte
}

func (fsm *fsmMock) Apply(cmd []byte) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.cmds = append(fsm.cmds, cmd)
	return "applied: " + string(cmd)
}

func (fsm *fsmMock) lastCommand() []byte {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.cmds) == 0 {
		return nil
	}
	return fsm.cmds[len(fsm.cmds)-1]
}

// ---------------------------------------------

func (r *Raft) getState() state {
	var s state
	r.inspect(func(r *Raft) {
		s = r.state
	})
	return s
}
