package raft

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/santhosh-tekuri/fnet"
	"github.com/santhosh-tekuri/raft/inmem"
)

func TestRaft_Voting(t *testing.T) {
	debug("\nTestRaft_Voting --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	c.ensureLeader(c.leader().id)

	req := &voteRequest{}
	ldr.inspect(func(r Info) {
		req.term = r.Term()
		req.lastLogIndex = r.LastLogIndex()
		req.lastLogTerm = r.LastLogTerm()
	})

	var followers []string
	for _, node := range ldr.configs.Latest.Nodes {
		if node.Type == Voter && node.Addr != ldr.addr {
			followers = append(followers, node.Addr)
		}
	}

	// a follower that thinks there's a leader should vote for that leader
	req.candidateID = ldr.addr
	ldr.inspect(func(_ Info) {
		resp, err := ldr.requestVote(ldr.getConnPool(followers[0]), req)
		if err != nil {
			t.Logf("requestVote failed: %v", err)
			t.Fail()
		}
		if !resp.granted {
			t.Logf("voteGranted: got %t, want true", resp.granted)
			t.Fail()
		}
	})

	// a follower that thinks there's a leader shouldn't vote for a different candidate
	req.candidateID = followers[0]
	ldr.inspect(func(_ Info) {
		resp, err := ldr.requestVote(ldr.getConnPool(followers[1]), req)
		if err != nil {
			t.Logf("requestVote failed: %v", err)
			t.Fail()
		} else if resp.granted {
			t.Logf("voteGranted: got %t, want false", resp.granted)
			t.Fail()
		}
	})
	if t.Failed() {
		t.FailNow()
	}
}

func TestRaft_SingleNode(t *testing.T) {
	debug("\nTestRaft_SingleNode --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(1, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	// should be able to apply
	resp, err := ldr.waitApply("test", c.heartbeatTimeout)
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
	if idx := ldr.fsmMock().len(); idx != 1 {
		t.Fatalf("fsm.len: got %d want 1", idx)
	}

	// check that it is applied to the FSM
	if cmd := ldr.fsmMock().lastCommand(); cmd != "test" {
		t.Fatalf("fsm.lastCommand: got %s want test", cmd)
	}
}

func TestRaft_TripleNode(t *testing.T) {
	debug("\nTestRaft_TripleNode --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	// should agree on leader
	c.ensureLeader(ldr.id)

	// should be able to apply
	resp, err := ldr.waitApply("test", c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if resp.msg != "test" {
		t.Fatalf("apply response mismatch. got %s, want test", resp.msg)
	}
	if resp.index != 1 {
		t.Fatalf("fsmReplyIndex: got %d want 1", resp.index)
	}
	c.ensureFSMReplicated(1)
}

func TestRaft_LeaderFail(t *testing.T) {
	debug("\nTestRaft_LeaderFail --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	// should agree on leader
	c.ensureLeader(ldr.id)

	// should be able to apply
	resp, err := ldr.waitApply("test", c.heartbeatTimeout)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if resp.msg != "test" {
		t.Fatalf("apply response mismatch. got %s, want test", resp.msg)
	}
	if resp.index != 1 {
		t.Fatalf("fsmReplyIndex: got %d want 1", resp.index)
	}
	c.ensureFSMReplicated(1)

	// disconnect leader
	ldrTerm := ldr.Info().Term()
	c.disconnect(ldr)

	// leader should stepDown
	if !ldr.waitForState(c.longTimeout, Follower, Candidate) {
		t.Fatal("leader should stepDown")
	}

	// wait for new leader
	c.ensureStability()
	newLdr := c.leader()
	if newLdr == ldr {
		t.Fatalf("newLeader: got %s, want !=%s", newLdr.addr, ldr.addr)
	}

	// ensure leader term is greater
	if newLdrTerm := newLdr.Info().Term(); newLdrTerm <= ldrTerm {
		t.Fatalf("expected new leader term: newLdrTerm=%d, ldrTerm=%d", newLdrTerm, ldrTerm)
	}

	// apply should work not work on old leader
	_, err = ldr.waitApply("reject", c.heartbeatTimeout)
	if err, ok := err.(NotLeaderError); !ok {
		t.Fatalf("got %v, want NotLeaderError", err)
	} else if err.Leader != "" {
		t.Fatalf("got %s, want ", err.Leader)
	}

	// apply should work on new leader
	if _, err = newLdr.waitApply("accept", c.heartbeatTimeout); err != nil {
		t.Fatalf("got %v, want nil", err)
	}

	// reconnect the networks
	c.connect()
	c.ensureHealthy()

	// wait for log replication
	c.ensureFSMReplicated(2)

	// Check two entries are applied to the FSM
	c.ensureFSMSame([]string{"test", "accept"})
}

func TestRaft_BehindFollower(t *testing.T) {
	debug("\nTestRaft_BehindFollower --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	// should agree on leader
	c.ensureLeader(ldr.id)

	// disconnect one follower
	behind := c.followers()[0]
	c.disconnect(behind)

	// commit a lot of things
	for i := 0; i < 100; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("test%d", i)))
	}
	if _, err := ldr.waitApply("test100", c.longTimeout); err != nil {
		t.Fatal(err)
	}

	// reconnect the behind node
	c.connect()
	c.ensureHealthy()

	// ensure all the logs are the same
	c.ensureFSMReplicated(101)
	c.ensureFSMSame(nil)

	// Ensure one leader
	c.ensureLeader(c.leader().id)
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	debug("\nTestRaft_ApplyNonLeader --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	// should agree on leader
	c.ensureLeader(ldr.id)

	// apply should work not work on non-leader
	for _, r := range c.rr {
		if r != ldr {
			_, err := r.waitApply("reject", c.commitTimeout)
			if err, ok := err.(NotLeaderError); !ok {
				t.Fatalf("got %v, want NotLeaderError", err)
			} else if err.Leader != ldr.addr {
				t.Fatalf("got %s, want %s", err.Leader, ldr.addr)
			}
		}
	}
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	debug("\nTestRaft_ApplyConcurrent --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	// should agree on leader
	c.ensureLeader(ldr.id)

	// concurrently apply
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := ldr.waitApply(fmt.Sprintf("test%d", i), 0); err != nil {
				debug("FAIL got", err, "want nil")
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
	c.ensureFSMReplicated(100)
	c.ensureFSMSame(nil)
}

func TestRaft_Bootstrap(t *testing.T) {
	debug("\nTestRaft_Bootstrap --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)

loop:
	// drain electionAbortCh
	for {
		select {
		case <-electionAbortedCh:
		default:
			break loop
		}
	}

	// launch cluster without bootstrapping
	c.launch(3, false)
	defer c.shutdown()

	// all nodes should must abort election
	timeout := time.After(c.longTimeout)
	aborted := make(map[NodeID]bool)
	for i := 0; i < 3; i++ {
		select {
		case id := <-electionAbortedCh:
			if aborted[id] {
				t.Fatalf("aborted twice")
			}
			aborted[id] = true
		case <-timeout:
			t.Fatal("timout in waiting for abort election")
		}
	}

	// bootstrap one of the nodes
	nodes := make(map[NodeID]Node, 3)
	for _, r := range c.rr {
		nodes[r.id] = Node{ID: r.id, Addr: r.addr, Type: Voter}
	}
	if _, err := c.rr[0].waitTask(Bootstrap(nodes), c.longTimeout); err != nil {
		t.Fatal(err)
	}

	// the bootstrapped node should be the leader
	c.ensureHealthy()
	ldr := c.rr[0]
	c.ensureLeader(ldr.id)

	// should be able to apply
	if _, err := ldr.waitApply("hello", 0); err != nil {
		t.Fatal(err)
	}
	c.ensureFSMReplicated(1)
	c.ensureFSMSame([]string{"hello"})

	// ensure bootstrap fails if already bootstrapped
	if _, err := c.rr[0].waitTask(Bootstrap(nodes), c.longTimeout); err != ErrCantBootstrap {
		t.Fatalf("got %v, want %v", err, ErrCantBootstrap)
	}
	if _, err := c.rr[1].waitTask(Bootstrap(nodes), c.longTimeout); err != ErrCantBootstrap {
		t.Fatalf("got %v, want %v", err, ErrCantBootstrap)
	}

	// disconnect leader, and ensure that new leader is chosen
	c.disconnect(ldr)
	c.ensureStability()
	newLdr := c.leader()
	if newLdr == ldr {
		t.Fatalf("newLeader: got %s, want !=%s", newLdr.addr, ldr.addr)
	}
}

func TestRaft_LeaderLeaseExpire(t *testing.T) {
	debug("\nTestRaft_LeaderLeaseExpire --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(2, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	// should agree on leader
	c.ensureLeader(ldr.id)

	// #followers must be 1
	followers := c.followers()
	if len(followers) != 1 {
		t.Fatalf("#followers: got %d, want 1", len(followers))
	}

	// disconnect the follower now
	c.disconnect(followers[0])

	// the leader should stepDown within leaderLeaseTimeout
	if !ldr.waitForState(2*c.heartbeatTimeout, Follower, Candidate) {
		t.Fatal("leader did not stepDown")
	}

	// should be no leaders
	if n := len(c.getInState(Leader)); n != 0 {
		t.Fatalf("#leaders: got %d, want 0", n)
	}

	// Ensure both have cleared their leader
	if !followers[0].waitForState(2*c.heartbeatTimeout, Candidate) {
		t.Fatal("follower should have become candidate")
	}
	for _, r := range c.rr {
		if ldr := r.Info().LeaderID(); ldr != "" {
			t.Fatalf("%s.leader: got %s want ", r.id, ldr)
		}
	}
}

func TestRaft_Barrier(t *testing.T) {
	debug("\nTestRaft_Barrier --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()
	followers := c.followers()

	// should agree on leader
	c.ensureLeader(ldr.id)

	// commit a lot of things
	n := 100
	for i := 0; i < n; i++ {
		ldr.NewEntries() <- UpdateEntry([]byte(fmt.Sprintf("test%d", i)))
	}

	// wait for a barrier complete
	b := BarrierEntry()
	ldr.NewEntries() <- b
	<-b.Done()
	if b.Err() != nil {
		t.Fatalf("barrier failed: %v", b.Err())
	}

	// ensure leader fsm got all commands
	if got := ldr.fsmMock().len(); int(got) != n {
		t.Fatalf("#entries: got %d, want %d", got, n)
	}

	// ensure leader's lastLogIndex matches with at-least one of follower
	len0 := ldr.Info().LastLogIndex()
	len1 := followers[0].Info().LastLogIndex()
	len2 := followers[1].Info().LastLogIndex()
	if len0 != len1 && len0 != len2 {
		t.Fatalf("len0 %d, len1 %d, len2 %d", len0, len1, len2)
	}
}

// todo: test query entries
// todo: test removal of leader. ensure that leader replies
//       success configChange before shutting down

func TestMain(m *testing.M) {
	code := m.Run()

	// wait until all pending debug messages are printed to stdout
	debug("barrier")

	os.Exit(code)
}

// ---------------------------------------------

type cluster struct {
	*testing.T
	rr               []*Raft
	network          *fnet.Network
	heartbeatTimeout time.Duration
	longTimeout      time.Duration
	commitTimeout    time.Duration
	fsmChangedCh     chan struct{}
}

func newCluster(t *testing.T) *cluster {
	return &cluster{
		T:                t,
		heartbeatTimeout: 50 * time.Millisecond,
		longTimeout:      5 * time.Second,
		commitTimeout:    5 * time.Millisecond,
		fsmChangedCh:     make(chan struct{}, 1),
	}
}

func (c *cluster) launch(n int, bootstrap bool) {
	c.Helper()
	c.network = fnet.New()
	nodes := make(map[NodeID]Node, n)
	for i := 1; i <= n; i++ {
		id := NodeID("M" + strconv.Itoa(i))
		nodes[id] = Node{ID: id, Addr: string(id) + ":8888", Type: Voter}
	}

	c.rr = make([]*Raft, n)
	opt := Options{
		HeartbeatTimeout: c.heartbeatTimeout,
	}
	trace := Trace{
		StateChanged:    stateChanged,
		ElectionAborted: electionAborted,
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
		r, err := New(node.ID, node.Addr, opt, fsm, storage, trace)
		if err != nil {
			c.Fatal(err)
		}

		c.rr[i] = r
		i++

		// switch to fake transport
		host := c.network.Host(string(r.id))
		r.dialFn = host.DialTimeout

		l, err := host.Listen("tcp", node.Addr)
		if err != nil {
			c.Fatalf("raft.listen failed: %v", err)
		}
		go func() { _ = r.Serve(l) }()
	}
}

func (c *cluster) ensureStability() {
	c.Helper()
	limitTimer := time.NewTimer(c.longTimeout)
	startupTimeout := c.heartbeatTimeout
	electionTimer := time.NewTimer(startupTimeout + 2*c.heartbeatTimeout)

	select {
	case <-stateChangedCh:
	default:
	}

	for {
		select {
		case <-limitTimer.C:
			c.Fatal("cluster is not stable")
		case <-stateChangedCh:
			electionTimer.Reset(2 * c.heartbeatTimeout)
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

func (c *cluster) ensureHealthy() *Raft {
	c.Helper()
	c.ensureStability()
	ldr := c.leader()
	c.followers()
	return ldr
}

func (c *cluster) ensureLeader(leaderID NodeID) {
	c.Helper()
	for _, r := range c.rr {
		if got := r.Info().LeaderID(); got != leaderID {
			c.Fatalf("leader of %s: got %s, want %s", r.addr, got, leaderID)
		}
	}
}

func (c *cluster) waitForLeader(timeout time.Duration) {
	c.Helper()

	cond := func() bool {
		for _, r := range c.rr {
			if r.Info().State() == Leader {
				return true
			}
		}
		return false
	}

	if !ensureTimeout(cond, stateChangedCh, timeout) {
		c.Fatalf("waitForLeader timeout")
	}
}

func (c *cluster) fsmReplicated(len uint64) bool {
	for _, r := range c.rr {
		if got := r.fsmMock().len(); got != len {
			return false
		}
	}
	return true
}

func (c *cluster) ensureFSMReplicated(len uint64) {
	c.Helper()
	cond := func() bool {
		return c.fsmReplicated(len)
	}
	if !ensureTimeout(cond, c.fsmChangedCh, c.longTimeout) {
		c.Fatalf("ensure fsmReplicated(%d): timeout after %s", len, c.longTimeout)
	}
}

// if want==nil, we ensure all fsm are same
// if want!=nil, we ensure all fms has want
func (c *cluster) ensureFSMSame(want []string) {
	c.Helper()
	if want == nil {
		want = c.rr[0].fsmMock().commands()
	}
	for _, r := range c.rr {
		if got := r.fsmMock().commands(); !reflect.DeepEqual(got, want) {
			c.Fatalf("\n got %v\n want %v", got, want)
		}
	}
}

func (c *cluster) disconnect(r *Raft) {
	host, _, _ := net.SplitHostPort(r.addr)
	debug("-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<-8<- disconnecting", host)
	c.network.SetFirewall(fnet.Split([]string{host}, fnet.AllowAll))
}

func (c *cluster) connect() {
	debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ reconnecting")
	c.network.SetFirewall(fnet.AllowAll)
}

func (c *cluster) shutdown() {
	for _, r := range c.rr {
		r.Shutdown().Wait()
		debug(r.addr, "<< shutdown()")
	}
}

// ---------------------------------------------

// wait until state is one of given states
func (r *Raft) waitForState(timeout time.Duration, states ...State) bool {
	cond := func() bool {
		got := r.Info().State()
		for _, want := range states {
			if got == want {
				return true
			}
		}
		return false
	}
	return ensureTimeout(cond, stateChangedCh, timeout)
}

func (r *Raft) fsmMock() *fsmMock {
	return r.fsm.(*fsmMock)
}

func (r *Raft) waitTask(t Task, timeout time.Duration) (interface{}, error) {
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

// use zero timeout, to wait till reply received
func (r *Raft) waitApply(cmd string, timeout time.Duration) (fsmReply, error) {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	ne := UpdateEntry([]byte(cmd))
	select {
	case r.NewEntries() <- ne:
		break
	case <-timer:
		return fsmReply{}, fmt.Errorf("waitApply(%v): submit timedout", cmd)
	}
	select {
	case <-ne.Done():
		if ne.Err() != nil {
			return fsmReply{}, ne.Err()
		}
		return ne.Result().(fsmReply), nil
	case <-timer:
		return fsmReply{}, fmt.Errorf("waitApply(%v): result timedout", cmd)
	}
}

func (r *Raft) inspect(fn func(Info)) {
	_, _ = r.waitTask(Inspect(fn), 0)
}

// events ----------------------------------------------------------------------

var stateChangedCh = make(chan struct{}, 1)
var electionAbortedCh = make(chan NodeID, 10)
var stateChanged = func(_ Info) {
	notify(stateChangedCh)
}
var electionAborted = func(info Info, reason string) {
	debug(info.ID(), info.Term(), string(info.State()), "|", "electionAborted", reason)
	select {
	case electionAbortedCh <- info.ID():
	default:
	}
}

// ---------------------------------------------

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
	fsm.cmds = append(fsm.cmds, string(cmd))
	notify(fsm.changedCh)
	return fsmReply{string(cmd), len(fsm.cmds)}
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

func ensureTimeout(condition func() bool, ch <-chan struct{}, timeout time.Duration) bool {
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
