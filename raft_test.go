package raft

import (
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/santhosh-tekuri/fnet"
	"github.com/santhosh-tekuri/raft/inmem"
)

func TestRaft_Voting(t *testing.T) {
	Debug("\nTestRaft_Voting --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()
	followers := c.followers()

	c.ensureLeader(c.leader().ID())

	// a follower that thinks there's a leader should vote for that leader
	granted, err := RequestVote(ldr, followers[0])
	if err != nil {
		t.Fatalf("requestVote failed: %v", err)
	}
	if !granted {
		t.Fatalf("voteGranted: got %t, want true", granted)
	}

	// a follower that thinks there's a leader shouldn't vote for a different candidate
	granted, err = RequestVote(followers[0], followers[1])
	if err != nil {
		t.Fatalf("requestVote failed: %v", err)
	} else if granted {
		t.Fatalf("voteGranted: got %t, want false", granted)
	}
}

func TestRaft_SingleNode(t *testing.T) {
	Debug("\nTestRaft_SingleNode --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(1, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	// check response
	if resp.msg != "test" {
		t.Fatalf("apply response mismatch. got %s, want test", resp.msg)
	}

	// check index
	if resp.index != 1 {
		t.Fatalf("fsmReplyIndex: got %d want 1", resp.index)
	}
	if idx := fsm(ldr).len(); idx != 1 {
		t.Fatalf("fsm.len: got %d want 1", idx)
	}

	// check that it is applied to the FSM
	if cmd := fsm(ldr).lastCommand(); cmd != "test" {
		t.Fatalf("fsm.lastCommand: got %s want test", cmd)
	}

	// shutdown and restart with fresh fsm and new addr
	c.shutdown()
	fsm := &fsmMock{changedCh: c.fsmChangedCh}
	storage := c.storage[string(ldr.ID())]
	r, err := New(ldr.ID(), "localhost:0", c.opt, fsm, storage, Trace{})
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go r.Serve(l)
	defer func() {
		r.Shutdown().Wait()
	}()

	// wait for fsm restore
	fsmRestored := func() bool {
		info := r.Info()
		return info.LastLogIndex() == info.Committed()
	}
	if !ensureTimeoutSleep(fsmRestored, c.commitTimeout, c.longTimeout) {
		t.Fatal("fsm restore failed after restart")
	}

	// ensure that fsm has been restored from log
	if idx := fsm.len(); idx != 1 {
		t.Fatalf("fsm.len: got %d want 1", idx)
	}
	if cmd := fsm.lastCommand(); cmd != "test" {
		t.Fatalf("fsm.lastCommand: got %s want test", cmd)
	}
}

func TestRaft_Shutdown(t *testing.T) {
	Debug("\nTestRaft_Shutdown --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(1, true)

	// shutdown
	c.shutdown()

	// shutdown on stopped one, should work
	c.shutdown()
}

func TestRaft_TripleNode(t *testing.T) {
	Debug("\nTestRaft_TripleNode --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()

	// should agree on leader
	c.ensureLeader(ldr.ID())

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.heartbeatTimeout)
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
}

func TestRaft_LeaderFail(t *testing.T) {
	Debug("\nTestRaft_LeaderFail --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()

	// should agree on leader
	c.ensureLeader(ldr.ID())

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.heartbeatTimeout)
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
	c.waitForStability(c.exclude(ldr)...)
	newLdr := c.leader()
	if newLdr == ldr {
		t.Fatalf("newLeader: got %s, want !=%s", newLdr.ID(), ldr.ID())
	}

	// ensure leader term is greater
	if newLdrTerm := newLdr.Info().Term(); newLdrTerm <= ldrTerm {
		t.Fatalf("expected new leader term: newLdrTerm=%d, ldrTerm=%d", newLdrTerm, ldrTerm)
	}

	// apply should not work on old leader
	_, err = waitUpdate(ldr, "reject", c.heartbeatTimeout)
	if err, ok := err.(NotLeaderError); !ok {
		t.Fatalf("got %v, want NotLeaderError", err)
	} else if err.Leader != "" {
		t.Fatalf("got %s, want ", err.Leader)
	}

	// apply should work on new leader
	if _, err = waitUpdate(newLdr, "accept", c.heartbeatTimeout); err != nil {
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

func TestRaft_BehindFollower(t *testing.T) {
	Debug("\nTestRaft_BehindFollower --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()

	// should agree on leader
	c.ensureLeader(ldr.ID())

	// disconnect one follower
	behind := c.followers()[0]
	c.disconnect(behind)

	// commit a lot of things
	for i := 0; i < 100; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("test%d", i)))
	}
	if _, err := waitUpdate(ldr, "test100", c.longTimeout); err != nil {
		t.Fatal(err)
	}

	// reconnect the behind node
	c.connect()
	c.waitForHealthy()

	// ensure all the logs are the same
	c.waitFSMLen(101)
	c.ensureFSMSame(nil)

	// Ensure one leader
	c.ensureLeader(c.leader().ID())
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	Debug("\nTestRaft_ApplyNonLeader --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()

	// should agree on leader
	c.ensureLeader(ldr.ID())

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
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()

	// should agree on leader
	c.ensureLeader(ldr.ID())

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

func TestRaft_Bootstrap(t *testing.T) {
	Debug("\nTestRaft_Bootstrap --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)

	electionAborted := c.registerForEvent(electionAborted)
	defer c.unregisterObserver(electionAborted)

	// launch cluster without bootstrapping
	c.launch(3, false)
	defer c.shutdown()

	// all nodes should must abort election and only once
	timeout := time.After(c.longTimeout)
	aborted := make(map[NodeID]bool)
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
	nodes := make(map[NodeID]Node, 3)
	for id, r := range c.rr {
		nodes[r.ID()] = Node{ID: r.ID(), Addr: id + ":8888", Type: Voter}
	}
	if err := waitBootstrap(c.rr["M1"], nodes, c.longTimeout); err != nil {
		t.Fatal(err)
	}

	// the bootstrapped node should be the leader
	c.waitForHealthy()
	ldr := c.rr["M1"]
	c.ensureLeader(ldr.ID())

	// should be able to apply
	if _, err := waitUpdate(ldr, "hello", 0); err != nil {
		t.Fatal(err)
	}
	c.waitFSMLen(1)
	c.ensureFSMSame([]string{"hello"})

	// ensure bootstrap fails if already bootstrapped
	if err := waitBootstrap(c.rr["M1"], nodes, c.longTimeout); err != ErrCantBootstrap {
		t.Fatalf("got %v, want %v", err, ErrCantBootstrap)
	}
	if err := waitBootstrap(c.rr["M2"], nodes, c.longTimeout); err != ErrCantBootstrap {
		t.Fatalf("got %v, want %v", err, ErrCantBootstrap)
	}

	// disconnect leader, and ensure that new leader is chosen
	c.disconnect(ldr)
	c.waitForStability(c.exclude(ldr)...)
	newLdr := c.leader()
	if newLdr == ldr {
		t.Fatalf("newLeader: got %s, want !=%s", newLdr.ID(), ldr.ID())
	}
}

func TestRaft_LeaderLeaseExpire(t *testing.T) {
	Debug("\nTestRaft_LeaderLeaseExpire --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(2, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()

	// should agree on leader
	c.ensureLeader(ldr.ID())

	// #followers must be 1
	followers := c.followers()
	if len(followers) != 1 {
		t.Fatalf("#followers: got %d, want 1", len(followers))
	}

	// disconnect the follower now
	c.disconnect(followers[0])

	// the leader should stepDown within leaderLeaseTimeout
	c.waitForState(ldr, 2*c.heartbeatTimeout, Follower, Candidate)

	// should be no leaders
	if n := len(c.getInState(Leader)); n != 0 {
		t.Fatalf("#leaders: got %d, want 0", n)
	}

	// Ensure both have cleared their leader
	c.waitForState(followers[0], 2*c.heartbeatTimeout, Candidate)
	c.ensureLeader(NodeID(""))
}

func TestRaft_Barrier(t *testing.T) {
	Debug("\nTestRaft_Barrier --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()
	followers := c.followers()

	// should agree on leader
	c.ensureLeader(ldr.ID())

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
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.waitForHealthy()

	// should agree on leader
	c.ensureLeader(ldr.ID())

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

// todo: test that non voter does not start election
//        * if he started as voter and hasn't got any requests from leader
//        * if leader contact lost for more than heartbeat timeout

// todo: test removal of leader, removal of follower
//       ensure that leader replies confChange
//       ensure that removed node sits idle as follower

func TestMain(m *testing.M) {
	code := m.Run()

	// wait until all pending debug messages are printed to stdout
	Debug("barrier")

	os.Exit(code)
}

// ---------------------------------------------

type cluster struct {
	*testing.T
	rr               map[string]*Raft
	storage          map[string]*Storage
	network          *fnet.Network
	heartbeatTimeout time.Duration
	longTimeout      time.Duration
	commitTimeout    time.Duration
	opt              Options
	fsmChangedCh     chan struct{}

	trace       Trace
	observersMu sync.RWMutex
	observerID  int
	observers   map[int]observer
}

func newCluster(t *testing.T) *cluster {
	heartbeatTimeout := 50 * time.Millisecond
	opt := Options{
		HeartbeatTimeout: heartbeatTimeout,
	}
	c := &cluster{
		T:                t,
		rr:               make(map[string]*Raft),
		storage:          make(map[string]*Storage),
		heartbeatTimeout: heartbeatTimeout,
		longTimeout:      5 * time.Second,
		commitTimeout:    5 * time.Millisecond,
		opt:              opt,
		fsmChangedCh:     make(chan struct{}, 1),
		observers:        make(map[int]observer),
	}
	c.trace = Trace{
		StateChanged:    c.onStateChanged,
		ElectionStarted: c.onElectionStarted,
		ElectionAborted: c.onElectionAborted,
		ConfigChanged:   c.onConfigChanged,
		ConfigCommitted: c.onConfigCommitted,
		ConfigReverted:  c.onConfigReverted,
		Unreachable:     c.onUnreachable,
	}
	return c
}

func (c *cluster) exclude(excludes ...*Raft) []*Raft {
	var members []*Raft

loop:
	for _, r := range c.rr {
		for _, exclude := range excludes {
			if r == exclude {
				continue loop
			}
		}
		members = append(members, r)
	}
	return members
}

func (c *cluster) registerObserver(filter func(event) bool) observer {
	c.observersMu.Lock()
	defer c.observersMu.Unlock()
	ob := observer{
		filter: filter,
		ch:     make(chan event, 1000),
		id:     c.observerID,
	}
	c.observers[ob.id] = ob
	c.observerID++
	return ob
}

func (c *cluster) registerForEvent(typ eventType, rr ...*Raft) observer {
	return c.registerObserver(func(e event) bool {
		if e.typ == typ {
			if len(rr) == 0 {
				return true
			}
			for _, r := range rr {
				if e.src == r.ID() {
					return true
				}
			}
		}
		return false
	})
}

func (c *cluster) unregisterObserver(ob observer) {
	c.observersMu.Lock()
	defer c.observersMu.Unlock()
	delete(c.observers, ob.id)
}

func (c *cluster) sendEvent(e event) {
	c.observersMu.RLock()
	defer c.observersMu.RUnlock()
	for _, ob := range c.observers {
		if ob.filter(e) {
			ob.ch <- e
		}
	}
}

func (c *cluster) onStateChanged(info Info) {
	c.sendEvent(event{
		src:   info.ID(),
		typ:   stateChanged,
		state: info.State(),
	})
}

func (c *cluster) onElectionStarted(info Info) {
	c.sendEvent(event{
		src: info.ID(),
		typ: electionStarted,
	})
}

func (c *cluster) onElectionAborted(info Info, reason string) {
	c.sendEvent(event{
		src: info.ID(),
		typ: electionAborted,
	})
}

func (c *cluster) onConfigChanged(info Info) {
	c.sendEvent(event{
		src:     info.ID(),
		typ:     configChanged,
		configs: info.Configs(),
	})
}

func (c *cluster) onConfigCommitted(info Info) {
	c.sendEvent(event{
		src:     info.ID(),
		typ:     configCommitted,
		configs: info.Configs(),
	})
}

func (c *cluster) onConfigReverted(info Info) {
	c.sendEvent(event{
		src:     info.ID(),
		typ:     configReverted,
		configs: info.Configs(),
	})
}

func (c *cluster) onUnreachable(info Info, id NodeID, since time.Time) {
	c.sendEvent(event{
		src:    info.ID(),
		typ:    unreachable,
		target: id,
		since:  since,
	})
}

func (c *cluster) launch(n int, bootstrap bool) {
	c.Helper()
	c.network = fnet.New()
	nodes := make(map[NodeID]Node, n)
	for i := 1; i <= n; i++ {
		id := NodeID("M" + strconv.Itoa(i))
		nodes[id] = Node{ID: id, Addr: string(id) + ":8888", Type: Voter}
	}

	i := 0
	for _, node := range nodes {
		inMemStorage := new(inmem.Storage)
		storage := NewStorage(inMemStorage, inMemStorage)
		if bootstrap {
			_, err := storage.bootstrap(nodes)
			if err != nil {
				c.Fatalf("Storage.bootstrap failed: %v", err)
			}
		}
		fsm := &fsmMock{changedCh: c.fsmChangedCh}
		r, err := New(node.ID, node.Addr, c.opt, fsm, storage, c.trace)
		if err != nil {
			c.Fatal(err)
		}

		c.rr[string(node.ID)] = r
		c.storage[string(node.ID)] = storage
		i++

		// switch to fake transport
		host := c.network.Host(string(r.ID()))
		r.dialFn = host.DialTimeout

		l, err := host.Listen("tcp", node.Addr)
		if err != nil {
			c.Fatalf("raft.listen failed: %v", err)
		}
		go func() { _ = r.Serve(l) }()
	}
}

func (c *cluster) waitForStability(members ...*Raft) {
	c.Helper()
	stateChanged := c.registerForEvent(stateChanged, members...)
	defer c.unregisterObserver(stateChanged)
	electionStarted := c.registerForEvent(electionStarted, members...)
	defer c.unregisterObserver(electionStarted)

	limitTimer := time.NewTimer(c.longTimeout)
	electionTimer := time.NewTimer(4 * c.heartbeatTimeout)
	for {
		select {
		case <-limitTimer.C:
			c.Fatal("cluster is not stable")
		case <-stateChanged.ch:
			electionTimer.Reset(4 * c.heartbeatTimeout)
		case <-electionStarted.ch:
			electionTimer.Reset(4 * c.heartbeatTimeout)
		case <-electionTimer.C:
			return
		}
	}
}

func (c *cluster) getInState(state State) []*Raft {
	var rr []*Raft
	for _, r := range c.rr {
		if r.Info().State() == state {
			rr = append(rr, r)
		}
	}
	return rr
}

func (c *cluster) leader() *Raft {
	c.Helper()
	leaders := c.getInState(Leader)
	if len(leaders) != 1 {
		c.Fatalf("got %d leaders, want 1 leader", len(leaders))
	}
	return leaders[0]
}

func (c *cluster) followers() []*Raft {
	c.Helper()
	followers := c.getInState(Follower)
	if len(followers) != len(c.rr)-1 {
		c.Fatalf("got %d followers, want %d followers", len(followers), len(c.rr)-1)
	}
	return followers
}

func (c *cluster) waitForHealthy() *Raft {
	c.Helper()
	c.waitForStability()
	ldr := c.leader()
	c.followers()
	return ldr
}

func (c *cluster) ensureLeader(leaderID NodeID) {
	c.Helper()
	for _, r := range c.rr {
		if got := r.Info().LeaderID(); got != leaderID {
			c.Fatalf("leader of %s: got %s, want %s", r.ID(), got, leaderID)
		}
	}
}

// wait until state is one of given states
func (c *cluster) waitForState(r *Raft, timeout time.Duration, states ...State) {
	condition := func() bool {
		got := r.Info().State()
		for _, want := range states {
			if got == want {
				return true
			}
		}
		return false
	}
	stateChanged := c.registerForEvent(stateChanged)
	defer c.unregisterObserver(stateChanged)
	if !stateChanged.waitFor(condition, timeout) {
		c.Fatalf("waitForState(%s, %v) timeout", r.ID(), states)
	}
}

func (c *cluster) waitForLeader(timeout time.Duration) {
	c.Helper()

	condition := func() bool {
		for _, r := range c.rr {
			if r.Info().State() == Leader {
				return true
			}
		}
		return false
	}
	stateChanged := c.registerForEvent(stateChanged)
	defer c.unregisterObserver(stateChanged)
	if !stateChanged.waitFor(condition, timeout) {
		c.Fatalf("waitForLeader timeout")
	}
}

func (c *cluster) fsmReplicated(len uint64) bool {
	for _, r := range c.rr {
		if got := fsm(r).len(); got != len {
			return false
		}
	}
	return true
}

func (c *cluster) waitFSMLen(len uint64) {
	c.Helper()
	cond := func() bool {
		return c.fsmReplicated(len)
	}
	if !ensureTimeoutCh(cond, c.fsmChangedCh, c.longTimeout) {
		c.Fatalf("ensure fsmReplicated(%d): timeout after %s", len, c.longTimeout)
	}
}

// if want==nil, we ensure all fsm are same
// if want!=nil, we ensure all fms has want
func (c *cluster) ensureFSMSame(want []string) {
	c.Helper()
	if want == nil {
		want = fsm(c.rr["M1"]).commands()
	}
	for _, r := range c.rr {
		if got := fsm(r).commands(); !reflect.DeepEqual(got, want) {
			c.Fatalf("\n got %v\n want %v", got, want)
		}
	}
}

func (c *cluster) disconnect(r *Raft) {
	host := string(r.ID())
	Debug("-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<- disconnecting", host)
	c.network.SetFirewall(fnet.Split([]string{host}, fnet.AllowAll))
}

func (c *cluster) connect() {
	Debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ reconnecting")
	c.network.SetFirewall(fnet.AllowAll)
}

func (c *cluster) shutdown() {
	for _, r := range c.rr {
		r.Shutdown().Wait()
		Debug(r.ID(), "<< shutdown()")
	}
}

// ---------------------------------------------

func fsm(r *Raft) *fsmMock {
	return r.FSM().(*fsmMock)
}

func waitTask(r *Raft, t Task, timeout time.Duration) (interface{}, error) {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	select {
	case r.Tasks() <- t:
		break
	case <-timer:
		return nil, fmt.Errorf("waitTask(%v): submit timedout", t)
	}
	select {
	case <-t.Done():
		return t.Result(), t.Err()
	case <-timer:
		return nil, fmt.Errorf("waitTask(%v): result timedout", t)
	}
}

func waitBootstrap(r *Raft, nodes map[NodeID]Node, timeout time.Duration) error {
	_, err := waitTask(r, Bootstrap(nodes), timeout)
	return err
}

func waitInspect(r *Raft, fn func(Info)) {
	_, _ = waitTask(r, Inspect(fn), 0)
}

// use zero timeout, to wait till reply received
func waitNewEntry(r *Raft, ne NewEntry, timeout time.Duration) (fsmReply, error) {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	select {
	case r.NewEntries() <- ne:
		break
	case <-timer:
		return fsmReply{}, errors.New("waitNewEntry: submit timedout")
	}
	select {
	case <-ne.Done():
		if ne.Err() != nil {
			return fsmReply{}, ne.Err()
		}
		result := fsmReply{}
		if ne.Result() != nil {
			result = ne.Result().(fsmReply)
		}
		return result, nil
	case <-timer:
		return fsmReply{}, errors.New("waitUpdate: result timeout")
	}
}

func waitUpdate(r *Raft, cmd string, timeout time.Duration) (fsmReply, error) {
	return waitNewEntry(r, UpdateEntry([]byte(cmd)), timeout)
}

func waitQuery(r *Raft, query string, timeout time.Duration) (fsmReply, error) {
	return waitNewEntry(r, QueryEntry([]byte(query)), timeout)
}

func waitBarrier(r *Raft, timeout time.Duration) error {
	_, err := waitNewEntry(r, BarrierEntry(), timeout)
	return err
}

// trace ----------------------------------------------------------------------

type eventType int

const (
	stateChanged eventType = iota
	electionStarted
	electionAborted
	configChanged
	configCommitted
	configReverted
	unreachable
)

type event struct {
	src NodeID
	typ eventType

	state   State
	configs Configs
	target  NodeID
	since   time.Time
}

type observer struct {
	filter func(event) bool
	ch     chan event
	id     int
}

func (ob observer) waitFor(condition func() bool, timeout time.Duration) bool {
	var timeoutCh <-chan time.Time
	if timeout <= 0 {
		timeoutCh = make(chan time.Time)
	}
	timeoutCh = time.After(timeout)

	if condition() {
		return true
	}
	for {
		select {
		case <-ob.ch:
			if condition() {
				return true
			}
		case <-timeoutCh:
			return false
		}
	}
}

//var (
//	stateChangedCh    = make(chan struct{}, 1)
//	configChangedCh   = make(chan struct{}, 1)
//	configCommittedCh = make(chan struct{}, 1)
//	configRevertedCh  = make(chan struct{}, 1)
//
//	electionStartedCh = make(chan NodeID, 1)
//	electionAbortedCh = make(chan NodeID, 10)
//	unreachableCh     = make(chan struct{}, 1)
//)
//
//func notifyTrace(ch chan<- struct{}) func(Info) {
//	return func(Info) {
//		notify(ch)
//	}
//}
//
//func electionStarted(info Info) {
//	select {
//	case electionAbortedCh <- info.ID():
//	default:
//	}
//}
//
//func electionAborted(info Info, reason string) {
//	Debug(info.ID(), info.Term(), string(info.State()), "|", "electionAborted", reason)
//	select {
//	case electionAbortedCh <- info.ID():
//	default:
//	}
//}
//
//func unreachable(Info, NodeID, time.Time) {
//	select {
//	case unreachableCh <- struct{}{}:
//	default:
//	}
//}
//
//var trace = Trace{
//	StateChanged:    notifyTrace(stateChangedCh),
//	ElectionStarted: electionStarted,
//	ElectionAborted: electionAborted,
//	ConfigChanged:   notifyTrace(configChangedCh),
//	ConfigCommitted: notifyTrace(configCommittedCh),
//	ConfigReverted:  notifyTrace(configRevertedCh),
//	Unreachable:     unreachable,
//}

// ---------------------------------------------

var errNoCommands = errors.New("no commands")
var errNoCommandAt = errors.New("no command at index")

type fsmMock struct {
	mu        sync.RWMutex
	cmds      []string
	changedCh chan<- struct{}
}

type fsmReply struct {
	msg   string
	index int
}

func (fsm *fsmMock) Apply(cmd []byte) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	s := string(cmd)

	// query
	if strings.HasPrefix(s, "query:") {
		s = strings.TrimPrefix(s, "query:")
		if s == "last" {
			sz := len(fsm.cmds)
			if sz == 0 {
				return errNoCommands
			}
			return fsmReply{fsm.cmds[sz-1], sz - 1}
		} else {
			i, err := strconv.Atoi(s)
			if err != nil {
				return err
			}
			if i < 0 || i >= len(fsm.cmds) {
				return errNoCommandAt
			}
			return fsmReply{fsm.cmds[i], i}
		}
	}

	// update
	fsm.cmds = append(fsm.cmds, s)
	notify(fsm.changedCh)
	return fsmReply{s, len(fsm.cmds)}
}

func (fsm *fsmMock) len() uint64 {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return uint64(len(fsm.cmds))
}

func (fsm *fsmMock) lastCommand() string {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.cmds) == 0 {
		return ""
	}
	return fsm.cmds[len(fsm.cmds)-1]
}

func (fsm *fsmMock) commands() []string {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return append([]string(nil), fsm.cmds...)
}

// ------------------------------------------------------------------

func notify(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func ensureTimeoutSleep(condition func() bool, sleep, timeout time.Duration) bool {
	limit := time.Now().Add(timeout)
	for time.Now().Before(limit) {
		if condition() {
			return true
		}
		time.Sleep(sleep)
	}
	return false
}

func ensureTimeoutCh(condition func() bool, ch <-chan struct{}, timeout time.Duration) bool {
	var timeoutCh <-chan time.Time
	if timeout <= 0 {
		timeoutCh = make(chan time.Time)
	}
	timeoutCh = time.After(timeout)

	select {
	case <-ch:
	default:
	}

	if condition() {
		return true
	}
	for {
		select {
		case <-ch:
			if condition() {
				return true
			}
		case <-timeoutCh:
			return false
		}
	}
}
