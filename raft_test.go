package raft

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/fnet"

	"github.com/santhosh-tekuri/raft/inmem"
)

func TestRaft(t *testing.T) {
	var err error
	cluster := new(cluster)
	if err := cluster.launch(3); err != nil {
		t.Fatal(err)
	}
	defer cluster.shutdown()

	var ldr *Raft
	t.Run("election should succeed", func(t *testing.T) {
		ldr, err = cluster.waitForLeader(10 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if ldr.getState() != leader {
			t.Fatal("leader lost leadership")
		}
	})

	t.Run("nonLeader should reject client requests with leaderID", func(t *testing.T) {
		// sleep so that followers recieve atleast one heatbeat from leader
		time.Sleep(2 * time.Second)
		for _, r := range cluster.rr {
			if r != ldr {
				_, err := r.waitApply("test", 10*time.Second)
				nlErr, ok := err.(NotLeaderError)
				if !ok {
					t.Fatal("non-leader should reply NotLeaderError")
				}
				if nlErr.Leader != ldr.addr {
					t.Fatalf("leaderId: got %s, want %s", nlErr.Leader, ldr.addr)
				}
			}
		}
	})

	cmd := "how are you?"
	t.Run("leader should apply client requests to fsm", func(t *testing.T) {
		debug(ldr, "raft.apply")
		resp, err := ldr.waitApply(cmd, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if resp != strings.ToUpper(cmd) {
			t.Fatalf("reply mismatch: got %q, want %q", resp, strings.ToUpper(cmd))
		}
	})

	t.Run("followers fsm should sync with leader", func(t *testing.T) {
		// sleep so that followers get cmd applied
		time.Sleep(10 * time.Second)
		for _, r := range cluster.rr {
			if last := r.fsm.(*fsmMock).lastCommand(); last != cmd {
				t.Errorf("%s lastCommand. got %q, want %q", r.getState(), last, cmd)
			}
		}
	})
}

// ---------------------------------------------

type fsmMock struct {
	mu   sync.RWMutex
	cmds []string
}

func (fsm *fsmMock) Apply(cmd []byte) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.cmds = append(fsm.cmds, string(cmd))
	return strings.ToUpper(string(cmd))
}

func (fsm *fsmMock) lastCommand() string {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.cmds) == 0 {
		return ""
	}
	return fsm.cmds[len(fsm.cmds)-1]
}

// ---------------------------------------------

type cluster struct {
	rr      []*Raft
	network *fnet.Network
}

func (c *cluster) launch(n int) error {
	c.network = fnet.New()
	addrs := make([]string, n)
	for i := range addrs {
		host := string('A' + i)
		c.network.AddTransport(host)
		addrs[i] = host + ":8888"
	}

	c.rr = make([]*Raft, n)
	for i := range c.rr {
		members := make([]string, n)
		copy(members, addrs)
		members[0], members[i] = members[i], members[0]
		storage := new(inmem.Storage)
		r := New(members, &fsmMock{}, storage, storage)
		c.rr[i] = r

		// switch to fnet transport
		host := c.network.Transport(string('A' + i))
		r.transport = host
		r.server.transport = host
		for _, m := range r.members {
			m.transport = host
		}

		if err := r.Listen(); err != nil {
			return fmt.Errorf("raft.listen failed: %v", err)
		}
		go r.Serve()

	}
	return nil
}

func (c *cluster) waitForLeader(timeout time.Duration) (*Raft, error) {
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

	// check if leader already chosen
	for _, r := range c.rr {
		if r.getState() == leader {
			return r, nil
		}
	}

	// wait until leader chosen
	select {
	case r := <-leaderCh:
		return r, nil
	case <-time.After(timeout):
		return nil, errors.New("waitForLeader: timedout")
	}
}

func (c *cluster) shutdown() {
	for _, r := range c.rr {
		r.Shutdown()
	}
}

// ---------------------------------------------

func (r *Raft) getState() state {
	var s state
	r.inspect(func(r *Raft) {
		s = r.state
	})
	return s
}

func (r *Raft) waitApply(cmd string, timeout time.Duration) (string, error) {
	respCh := make(chan interface{}, 1)
	r.Apply([]byte(cmd), respCh)
	select {
	case resp := <-respCh:
		if err, ok := resp.(error); ok {
			return "", err
		}
		return resp.(string), nil
	case <-time.After(timeout):
		return "", fmt.Errorf("waitApply(%q): timedout", cmd)
	}
}
