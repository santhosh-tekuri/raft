package raft

import (
	"errors"
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

	req := &voteRequest{}
	ldr.inspect(func(r *Raft) {
		req.term = r.term
		req.lastLogIndex = r.lastLogIndex
		req.lastLogTerm = r.lastLogTerm
	})

	var followers []string
	for _, node := range ldr.configs.Latest.Nodes {
		if node.Type == Voter && node.Addr != ldr.addr {
			followers = append(followers, node.Addr)
		}
	}

	// a follower that thinks there's a leader should vote for that leader
	req.candidateID = ldr.addr
	ldr.inspect(func(raft *Raft) {
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
	ldr.inspect(func(raft *Raft) {
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
	c.ensureLeader(ldr.addr)

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
	c.ensureLeader(ldr.addr)

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
	ldrTerm := ldr.Info().Term
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
	if newLdrTerm := newLdr.Info().Term; newLdrTerm <= ldrTerm {
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
	c.ensureLeader(ldr.addr)

	// disconnect one follower
	behind := c.followers()[0]
	c.disconnect(behind)

	// commit a lot of things
	for i := 0; i < 100; i++ {
		ldr.TasksCh <- ApplyCommand([]byte(fmt.Sprintf("test%d", i)))
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
	c.ensureLeader(c.leader().addr)
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	debug("\nTestRaft_ApplyNonLeader --------------------------")
	defer leaktest.Check(t)()
	c := newCluster(t)
	c.launch(3, true)
	defer c.shutdown()
	ldr := c.ensureHealthy()

	// should agree on leader
	c.ensureLeader(ldr.addr)

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
	c.ensureLeader(ldr.addr)

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

	// launch cluster without bootstrapping
	addrCh := make(chan string, 3)
	onElectionAborted(func(raft *Raft) {
		select {
		case addrCh <- raft.addr:
		default:
		}
	})
	defer onElectionAborted(nil)
	c.launch(3, false)
	defer c.shutdown()

	// all nodes should must abort election
	timeout := time.After(c.longTimeout)
	aborted := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		select {
		case addr := <-addrCh:
			aborted[addr] = struct{}{}
		case <-timeout:
			t.Fatal("timout in waiting for abort election")
		}
	}
	if len(aborted) != 3 {
		t.Fatalf("got %d, want 3", len(aborted))
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
	c.ensureLeader(ldr.addr)

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
	c.ensureLeader(ldr.addr)

	// #followers must be 1
	followers := c.followers()
	if len(followers) != 1 {
		t.Fatalf("#followers: got %d, want 1", len(followers))
	}

	// disconnect the follower now
	c.disconnect(followers[0])

	// the leader should stepDown within leaderLeaseTimeout
	ldr.waitForState(3*c.heartbeatTimeout, Follower, Candidate)

	// should be no leaders
	if n := len(c.getInState(Leader)); n != 0 {
		t.Fatalf("#leaders: got %d, want 0", n)
	}

	// Ensure both have cleared their leader
	followers[0].waitForState(2*c.heartbeatTimeout, Candidate)
	for _, r := range c.rr {
		if ldr := r.Info().Leader; ldr != "" {
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
	c.ensureLeader(ldr.addr)

	// commit a lot of things
	n := 100
	for i := 0; i < n; i++ {
		ldr.TasksCh <- ApplyCommand([]byte(fmt.Sprintf("test%d", i)))
	}

	// wait for a barrier complete
	b := Barrier()
	ldr.TasksCh <- b
	<-b.Done()
	if b.Err() != nil {
		t.Fatalf("barrier failed: %v", b.Err())
	}

	// ensure leader fsm got all commands
	if got := ldr.fsmMock().len(); int(got) != n {
		t.Fatalf("#entries: got %d, want %d", got, n)
	}

	// ensure leader's lastLogIndex matches with at-least one of follower
	len0 := ldr.Info().LastLogIndex
	len1 := followers[0].Info().LastLogIndex
	len2 := followers[1].Info().LastLogIndex
	if len0 != len1 && len0 != len2 {
		t.Fatalf("len0 %d, len1 %d, len2 %d", len0, len1, len2)
	}
}

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
	i := 0
	for _, node := range nodes {
		inMemStorage := new(inmem.Storage)
		if bootstrap {
			storage := storage{Stable: inMemStorage, log: inMemStorage}
			_, err := storage.bootstrap(nodes)
			if err != nil {
				c.Fatalf("storage.bootstrap failed: %v", err)
			}
		}
		fsm := &fsmMock{changedCh: c.fsmChangedCh}
		r, err := New(node.ID, node.Addr, fsm, inMemStorage, inMemStorage)
		if err != nil {
			c.Fatal(err)
		}

		r.heartbeatTimeout = c.heartbeatTimeout
		c.rr[i] = r
		i++

		// switch to fake transport
		host := c.network.Host(string(r.id))
		r.listenFn, r.dialFn = host.Listen, host.DialTimeout

		if err := r.Listen(); err != nil {
			c.Fatalf("raft.listen failed: %v", err)
		}
		go func() { _ = r.Serve() }()
	}
}

func (c *cluster) ensureStability() {
	c.Helper()
	limitTimer := time.NewTimer(c.longTimeout)
	startupTimeout := c.heartbeatTimeout
	electionTimer := time.NewTimer(startupTimeout + 2*c.heartbeatTimeout)

	stateChanged := make(chan struct{}, 10)
	onStateChange(func(r *Raft) {
		select {
		case stateChanged <- struct{}{}:
		default:
		}
	})
	defer onStateChange(nil)

	//onVoteRequest(func(r *Raft, req *voteRequest) {
	//	select {
	//	case stateChanged <- struct{}{}:
	//	default:
	//	}
	//})
	//defer onVoteRequest(nil)

	for {
		select {
		case <-limitTimer.C:
			c.Fatal("cluster is not stable")
		case <-stateChanged:
			electionTimer.Reset(2 * c.heartbeatTimeout)
		case <-electionTimer.C:
			return
		}
	}
}

func (c *cluster) getInState(state State) []*Raft {
	var rr []*Raft
	for _, r := range c.rr {
		if r.Info().State == state {
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

func (c *cluster) ensureLeader(leader string) {
	c.Helper()
	for _, r := range c.rr {
		if got := r.Info().Leader; got != leader {
			c.Fatalf("leader of %s: got %s, want %s", r.addr, got, leader)
		}
	}
}

func (c *cluster) waitForLeader(timeout time.Duration) (*Raft, error) {
	c.Helper()
	leaderCh := make(chan *Raft, 1)
	onStateChange(func(r *Raft) {
		if r.state == Leader {
			select {
			case leaderCh <- r:
			default:
			}
		}
	})
	defer onStateChange(nil)

	// check if leader already chosen
	for _, r := range c.rr {
		if r.Info().State == Leader {
			return r, nil
		}
	}

	// wait until leader chosen
	select {
	case r := <-leaderCh:
		return r, nil
	case <-time.After(timeout):
		return nil, errors.New("waitForLeader: timeout")
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
		r.Shutdown()
	}
}

// ---------------------------------------------

// wait until state is one of given states
func (r *Raft) waitForState(timeout time.Duration, states ...State) bool {
	statesSet := make(map[State]bool)
	for _, s := range states {
		statesSet[s] = true
	}

	ch := make(chan struct{}, 1)
	onStateChange(func(raft *Raft) {
		if raft == r && statesSet[r.state] {
			close(ch)
			onStateChange(nil)
		}
	})
	defer onStateChange(nil)
	if !statesSet[r.Info().State] {
		select {
		case <-ch:
			break
		case <-time.After(timeout):
			return false
		}
	}
	return true
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
	case r.TasksCh <- t:
		break
	case <-timer:
		return nil, fmt.Errorf("waitApply(%v): submit timedout", t)
	}
	select {
	case <-t.Done():
		return t.Result(), t.Err()
	case <-timer:
		return nil, fmt.Errorf("waitApply(%v): result timedout", t)
	}
}

// use zero timeout, to wait till reply received
func (r *Raft) waitApply(cmd string, timeout time.Duration) (fsmReply, error) {
	result, err := r.waitTask(ApplyCommand([]byte(cmd)), timeout)
	if err != nil {
		return fsmReply{}, err
	}
	return result.(fsmReply), nil
}

func (r *Raft) inspect(fn func(*Raft)) {
	_, _ = r.waitTask(inspect(fn), 0)
}

// events ----------------------------------------------------------------------

var mu sync.RWMutex
var stateChangedFn func(*Raft)
var electionAbortedFn func(*Raft)
var voteRequestFn func(*Raft, *voteRequest)

func init() {
	StateChanged = func(r *Raft, _ State) {
		mu.RLock()
		f := stateChangedFn
		mu.RUnlock()
		if f != nil {
			f(r)
		}
	}

	ElectionAborted = func(r *Raft, reason string) {
		mu.RLock()
		f := electionAbortedFn
		mu.RUnlock()
		debug(r, "electionAborted", reason)
		if f != nil {
			f(r)
		}
	}

	gotVoteRequest = func(r *Raft, req *voteRequest) {
		mu.RLock()
		f := voteRequestFn
		mu.RUnlock()
		if f != nil {
			f(r, req)
		}
	}
}

func onStateChange(f func(*Raft)) {
	mu.Lock()
	stateChangedFn = f
	mu.Unlock()
}

func onElectionAborted(f func(*Raft)) {
	mu.Lock()
	electionAbortedFn = f
	mu.Unlock()
}

func onVoteRequest(f func(*Raft, *voteRequest)) {
	mu.Lock()
	voteRequestFn = f
	mu.Unlock()
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
