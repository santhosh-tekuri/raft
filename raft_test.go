package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/santhosh-tekuri/fnet"
)

func TestRaft_Voting(t *testing.T) {
	Debug("\nTestRaft_Voting --------------------------")
	defer leaktest.Check(t)()
	c, ldr, followers := launchCluster(t, 3)
	defer c.shutdown()

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
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

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

	// shutdown and restart with fresh fsm
	r := c.restart(ldr)

	// ensure that fsm has been restored from log
	c.waitFSMLen(fsm(ldr).len(), r)
	if cmd := fsm(r).lastCommand(); cmd != "test" {
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
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

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
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

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
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

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
	aborted := make(map[ID]bool)
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
	nodes := make(map[ID]Node, 3)
	for id, r := range c.rr {
		nodes[r.ID()] = Node{ID: r.ID(), Addr: id + ":8888", Voter: true}
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
	if err := waitBootstrap(c.rr["M1"], nodes, c.longTimeout); err != ErrAlreadyBootstrapped {
		t.Fatalf("got %v, want %v", err, ErrAlreadyBootstrapped)
	}
	if err := waitBootstrap(c.rr["M2"], nodes, c.longTimeout); err != ErrAlreadyBootstrapped {
		t.Fatalf("got %v, want %v", err, ErrAlreadyBootstrapped)
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
	c, ldr, followers := launchCluster(t, 2)
	defer c.shutdown()

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
	c.ensureLeader(ID(""))
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

func launchCluster(t *testing.T, n int) (c *cluster, ldr *Raft, followers []*Raft) {
	t.Helper()
	c = newCluster(t)
	c.launch(n, true)
	ldr = c.waitForHealthy()
	c.ensureLeader(ldr.ID())
	followers = c.followers()
	return
}

func newCluster(t *testing.T) *cluster {
	heartbeatTimeout := 50 * time.Millisecond
	c := &cluster{
		T:                t,
		network:          fnet.New(),
		rr:               make(map[string]*Raft),
		storage:          make(map[string]Storage),
		heartbeatTimeout: heartbeatTimeout,
		longTimeout:      5 * time.Second,
		commitTimeout:    5 * time.Millisecond,
		observers:        make(map[int]observer),
	}
	c.opt = Options{
		HeartbeatTimeout:   heartbeatTimeout,
		LeaderLeaseTimeout: heartbeatTimeout,
		Trace: Trace{
			StateChanged:    c.onStateChanged,
			ElectionStarted: c.onElectionStarted,
			ElectionAborted: c.onElectionAborted,
			ConfigChanged:   c.onConfigChanged,
			ConfigCommitted: c.onConfigCommitted,
			ConfigReverted:  c.onConfigReverted,
			Unreachable:     c.onUnreachable,
		},
	}
	return c
}

type cluster struct {
	*testing.T
	rr               map[string]*Raft
	storage          map[string]Storage
	network          *fnet.Network
	heartbeatTimeout time.Duration
	longTimeout      time.Duration
	commitTimeout    time.Duration
	opt              Options

	observersMu sync.RWMutex
	observerID  int
	observers   map[int]observer
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
	typeMatches := func(want, got eventType) bool {
		return got == want
	}
	if typ == configRelated {
		typeMatches = func(want, got eventType) bool {
			return got == configChanged ||
				got == configCommitted ||
				got == configReverted
		}
	}
	return c.registerObserver(func(e event) bool {
		if typeMatches(typ, e.typ) {
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

func (c *cluster) onFMSChanded(id ID, len uint64) {
	c.sendEvent(event{
		src:    id,
		typ:    fsmChanged,
		fsmLen: len,
	})
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

func (c *cluster) onUnreachable(info Info, id ID, since time.Time) {
	c.sendEvent(event{
		src:    info.ID(),
		typ:    unreachable,
		target: id,
		since:  since,
	})
}

func (c *cluster) launch(n int, bootstrap bool) map[ID]*Raft {
	c.Helper()
	nodes := make(map[ID]Node, n)
	for i := 1; i <= n; i++ {
		id := ID("M" + strconv.Itoa(i+len(c.rr)))
		nodes[id] = Node{ID: id, Addr: string(id) + ":8888", Voter: true}
	}

	launched := make(map[ID]*Raft)
	for _, node := range nodes {
		s := new(inmemStorage)
		storage := Storage{Vars: s, Log: s, Snapshots: s}
		if bootstrap {
			if err := BootstrapStorage(storage, nodes); err != nil {
				c.Fatalf("Storage.bootstrap failed: %v", err)
			}
		}
		fsm := &fsmMock{id: node.ID, changed: c.onFMSChanded}
		r, err := New(node.ID, c.opt, fsm, storage)
		if err != nil {
			c.Fatal(err)
		}
		launched[r.ID()] = r
		c.rr[string(node.ID)] = r
		c.storage[string(node.ID)] = storage

		// switch to fake transport
		host := c.network.Host(string(r.ID()))
		r.dialFn = host.DialTimeout

		l, err := host.Listen("tcp", node.Addr)
		if err != nil {
			c.Fatalf("raft.listen failed: %v", err)
		}
		go func() { _ = r.Serve(l) }()
	}
	return launched
}

func (c *cluster) shutdown() {
	for _, r := range c.rr {
		r.Shutdown().Wait()
		Debug(r.ID(), "<< shutdown()")
	}
}

func (c *cluster) restart(r *Raft) *Raft {
	c.Helper()
	addr := r.Info().Addr()
	r.Shutdown().Wait()
	Debug(r.ID(), "<< shutdown()")

	newFSM := &fsmMock{id: r.ID(), changed: c.onFMSChanded}
	storage := c.storage[string(r.ID())]
	newr, err := New(r.ID(), c.opt, newFSM, storage)
	if err != nil {
		c.Fatal(err)
	}

	host := c.network.Host(string(r.ID()))
	newr.dialFn = host.DialTimeout

	l, err := host.Listen("tcp", addr)
	if err != nil {
		c.Fatal(err)
	}
	c.rr[string(r.ID())] = newr
	go newr.Serve(l)
	return newr
}

func (c *cluster) waitForStability(rr ...*Raft) {
	c.Helper()
	stateChanged := c.registerForEvent(stateChanged, rr...)
	defer c.unregisterObserver(stateChanged)
	electionStarted := c.registerForEvent(electionStarted, rr...)
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

func (c *cluster) getInState(state State, rr ...*Raft) []*Raft {
	if len(rr) == 0 {
		rr = c.exclude()
	}
	var inState []*Raft
	for _, r := range rr {
		if r.Info().State() == state {
			inState = append(inState, r)
		}
	}
	return inState
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

func (c *cluster) ensureLeader(leader ID) {
	c.Helper()
	for _, r := range c.rr {
		if got := r.Info().Leader(); got != leader {
			c.Fatalf("leader of %s: got %s, want %s", r.ID(), got, leader)
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

func (c *cluster) waitForLeader(timeout time.Duration, rr ...*Raft) *Raft {
	c.Helper()
	if len(rr) == 0 {
		rr = c.exclude()
	}
	condition := func() bool {
		for _, r := range rr {
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
	return c.getInState(Leader, rr...)[0]
}

func (c *cluster) waitFSMLen(fsmLen uint64, rr ...*Raft) {
	c.Helper()
	if len(rr) == 0 {
		rr = c.exclude()
	}
	condition := func() bool {
		for _, r := range rr {
			if got := fsm(r).len(); got != fsmLen {
				return false
			}
		}
		return true
	}
	fsmChanged := c.registerForEvent(fsmChanged, rr...)
	defer c.unregisterObserver(fsmChanged)
	if !fsmChanged.waitFor(condition, c.longTimeout) {
		c.Logf("fsmLen: want %d", fsmLen)
		for _, r := range rr {
			c.Logf("%s got %d", r.ID(), fsm(r).len())
		}
		c.Fatalf("waitFSMLen(%d) timeout", fsmLen)
	}
}

// if want==nil, we ensure all fsm are same
// if want!=nil, we ensure all fms has want
func (c *cluster) ensureFSMSame(want []string, rr ...*Raft) {
	c.Helper()
	if len(rr) == 0 {
		rr = c.exclude()
	}
	if want == nil {
		for _, r := range rr {
			want = fsm(r).commands()
			break
		}
	}
	for _, r := range rr {
		if got := fsm(r).commands(); !reflect.DeepEqual(got, want) {
			c.Fatalf("\n got %v\n want %v", got, want)
		}
	}
}

func (c *cluster) waitCatchup(r *Raft) {
	c.Helper()
	leaders := c.getInState(Leader)
	if len(leaders) != 1 {
		c.Fatalf("leaders: got %d, want 1", len(leaders))
	}
	ldrLastLogIndex := leaders[0].Info().LastLogIndex()
	condition := func() bool {
		info := r.Info()
		return info.Committed() == ldrLastLogIndex
	}
	if !waitForCondition(condition, c.commitTimeout, c.longTimeout) {
		info := r.Info()
		c.Fatalf("%s catchup: got %d, want %d", info.ID(), info.Committed(), ldrLastLogIndex)
	}
}

func (c *cluster) waitUnreachableDetected(ldr, failed *Raft) {
	c.Helper()
	condition := func() bool {
		return !ldr.Info().Replication()[failed.ID()].Unreachable.IsZero()
	}
	unreachable := c.registerForEvent(unreachable, ldr)
	defer c.unregisterObserver(unreachable)
	if !unreachable.waitFor(condition, c.longTimeout) {
		c.Fatalf("waitUnreachableDetected: ldr %s failed %s", ldr.ID(), failed.ID())
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

func waitBootstrap(r *Raft, nodes map[ID]Node, timeout time.Duration) error {
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
	fsmChanged eventType = iota
	stateChanged
	electionStarted
	electionAborted
	configChanged
	configCommitted
	configReverted
	unreachable

	configRelated
)

type event struct {
	src ID
	typ eventType

	fsmLen  uint64
	state   State
	configs Configs
	target  ID
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

// ---------------------------------------------

var errNoCommands = errors.New("no commands")
var errNoCommandAt = errors.New("no command at index")

type fsmMock struct {
	id      ID
	mu      sync.RWMutex
	cmds    []string
	changed func(id ID, len uint64)
}

var _ FSM = (*fsmMock)(nil)

type fsmReply struct {
	msg   string
	index int
}

func (fsm *fsmMock) Execute(cmd []byte) interface{} {
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
	if fsm.changed != nil {
		fsm.changed(fsm.id, uint64(len(fsm.cmds)))
	}
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

type stateMock struct {
	cmds []string
}

func (state stateMock) WriteTo(w io.Writer) error {
	if err := gob.NewEncoder(w).Encode(state.cmds); err != nil {
		return err
	}
	return nil
}

func (state stateMock) Release() {}

func (fsm *fsmMock) Snapshot() (FSMState, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return stateMock{fsm.cmds}, nil
}

func (fsm *fsmMock) RestoreFrom(r io.Reader) error {
	var cmds []string
	if err := gob.NewDecoder(r).Decode(&cmds); err != nil {
		return err
	}
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.cmds = cmds
	if fsm.changed != nil {
		fsm.changed(fsm.id, uint64(len(cmds)))
	}
	return nil
}

// ------------------------------------------------------------------

var (
	ErrNotFound   = errors.New("not found")
	ErrOutOfRange = errors.New("out of range")
)

type inmemStorage struct {
	muStable      sync.RWMutex
	term          uint64
	vote          string
	confCommitted uint64
	confLatest    uint64

	muLog sync.RWMutex
	list  [][]byte

	snapMeta SnapshotMeta
	snapshot *bytes.Buffer
}

func (s *inmemStorage) GetVote() (term uint64, vote string, err error) {
	s.muStable.RLock()
	defer s.muStable.RUnlock()
	return s.term, s.vote, nil
}

func (s *inmemStorage) SetVote(term uint64, vote string) error {
	s.muStable.Lock()
	defer s.muStable.Unlock()
	s.term, s.vote = term, vote
	return nil
}

func (s *inmemStorage) GetConfig() (committed, latest uint64, err error) {
	s.muStable.RLock()
	defer s.muStable.RUnlock()
	return s.confCommitted, s.confLatest, nil
}

func (s *inmemStorage) SetConfig(committed, latest uint64) error {
	s.muStable.Lock()
	defer s.muStable.Unlock()
	s.confCommitted, s.confLatest = committed, latest
	return nil
}

func (s *inmemStorage) Empty() (bool, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	return len(s.list) == 0, nil
}

func (s *inmemStorage) First() ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if len(s.list) == 0 {
		return nil, ErrNotFound
	}
	return s.list[0], nil
}

func (s *inmemStorage) Last() ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if len(s.list) == 0 {
		return nil, ErrNotFound
	}
	return s.list[len(s.list)-1], nil
}

func (s *inmemStorage) Get(offset uint64) ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if offset >= uint64(len(s.list)) {
		return nil, ErrNotFound
	}
	return s.list[offset], nil
}

func (s *inmemStorage) Append(entry []byte) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	s.list = append(s.list, entry)
	return nil
}

func (s *inmemStorage) DeleteFirst(n uint64) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	if n > uint64(len(s.list)) {
		return ErrOutOfRange
	}
	s.list = s.list[n:]
	return nil
}

func (s *inmemStorage) DeleteLast(n uint64) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	if n > uint64(len(s.list)) {
		return ErrOutOfRange
	}
	s.list = s.list[:len(s.list)-int(n)]
	return nil
}

type inmemSink struct {
	s    *inmemStorage
	meta SnapshotMeta
	*bytes.Buffer
}

func (s *inmemSink) Done(err error) (SnapshotMeta, error) {
	if err == nil {
		s.meta.Size = int64(s.Buffer.Len())
		s.s.snapMeta = s.meta
		s.s.snapshot = s.Buffer
	}
	return s.meta, err
}

func (s *inmemStorage) New(index, term uint64, config Config) (SnapshotSink, error) {
	return &inmemSink{
		s: s,
		meta: SnapshotMeta{
			Index:  index,
			Term:   term,
			Config: config,
		},
		Buffer: new(bytes.Buffer),
	}, nil
}

func (s *inmemStorage) List() ([]uint64, error) {
	if s.snapMeta.Index == 0 {
		return nil, nil
	}
	return []uint64{s.snapMeta.Index}, nil
}

func (s *inmemStorage) Meta(index uint64) (SnapshotMeta, error) {
	if index != s.snapMeta.Index {
		return SnapshotMeta{}, fmt.Errorf("no snapshot found for index %d", index)
	}
	return s.snapMeta, nil
}

func (s *inmemStorage) Open(index uint64) (SnapshotMeta, io.ReadCloser, error) {
	if index != s.snapMeta.Index {
		return SnapshotMeta{}, nil, fmt.Errorf("no snapshot found for index %d", index)
	}
	return s.snapMeta, ioutil.NopCloser(bytes.NewReader(s.snapshot.Bytes())), nil
}

// ------------------------------------------------------------------

func waitForCondition(condition func() bool, sleep, timeout time.Duration) bool {
	limit := time.Now().Add(timeout)
	for time.Now().Before(limit) {
		if condition() {
			return true
		}
		time.Sleep(sleep)
	}
	return false
}
