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
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/santhosh-tekuri/fnet"
)

func TestRaft_shutdown_once(t *testing.T) {
	c := newCluster(t)
	c.launch(1, true)
	c.shutdown()
}

func TestRaft_shutdown_twice(t *testing.T) {
	c := newCluster(t)
	c.launch(1, true)
	c.shutdown()
	c.shutdown()
}

func TestRaft_bootstrap(t *testing.T) {
	c := newCluster(t)

	electionAborted := c.registerFor(electionAborted)
	defer c.unregister(electionAborted)

	// launch cluster without bootstrapping
	c.launch(3, false)
	defer c.shutdown()

	// all nodes should must abort election and only once
	tdebug("waitForElectionAborted")
	timeout := time.After(c.longTimeout)
	aborted := make(map[uint64]bool)
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
	tdebug("prepareBootStrapConfig")
	ldr := c.rr[1]
	config := ldr.Info().Configs().Latest
	for _, r := range c.rr {
		if err := config.AddVoter(r.NID(), c.id2Addr(r.NID())); err != nil {
			c.Fatal(err)
		}
	}
	if err := waitBootstrap(ldr, config, c.longTimeout); err != nil {
		t.Fatal(err)
	}

	// the bootstrapped node should be the leader
	c.waitForLeader(ldr)
	c.waitForFollowers()

	// should be able to apply
	if _, err := waitUpdate(ldr, "hello", 0); err != nil {
		t.Fatal(err)
	}
	c.waitFSMLen(1)
	c.ensureFSMSame([]string{"hello"})

	// ensure bootstrap fails if already bootstrapped
	if err := waitBootstrap(ldr, config, c.longTimeout); err != ErrConfigChanged {
		t.Fatalf("got %v, want %v", err, ErrConfigChanged)
	}
	err := waitBootstrap(c.rr[2], config, c.longTimeout)
	if _, ok := err.(NotLeaderError); !ok {
		t.Fatalf("got %v, want NotLeaderError", err)
	}

	// disconnect leader, and ensure that new leader is chosen
	c.disconnect(ldr)
	c.waitForLeader(c.exclude(ldr)...)
}

func TestRaft_singleNode(t *testing.T) {
	c, ldr, _ := launchCluster(t, 1)
	defer c.shutdown()

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.longTimeout)
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

func TestRaft_tripleNode(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	// should be able to apply
	resp, err := waitUpdate(ldr, "test", c.longTimeout)
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

// todo: test that non voter does not start election
//        * if he started as voter and hasn't got any requests from leader
//        * if leader contact lost for more than heartbeat timeout

// todo: test removal of leader, removal of follower
//       ensure that leader replies confChange
//       ensure that removed node sits idle as follower

// ---------------------------------------------------------------------------------------------

var tempDir string

func TestMain(m *testing.M) {
	testMode = true
	temp, err := ioutil.TempDir("", "log")
	if err != nil {
		os.Exit(1)
	}
	tempDir = temp
	code := m.Run()
	debug("barrier") // wait until all pending debug messages are printed to stdout
	_ = os.RemoveAll(tempDir)
	os.Exit(code)
}

// ---------------------------------------------

func host(r *Raft) string {
	return id2Host(r.NID())
}

func id2Host(id uint64) string {
	return fmt.Sprintf("M%d", id)
}

func (c *cluster) id2Addr(id uint64) string {
	port, ok := c.ports[id]
	if !ok {
		port = c.port
	}
	return fmt.Sprintf("%s:%d", id2Host(id), port)
}

func launchCluster(t *testing.T, n int) (c *cluster, ldr *Raft, flrs []*Raft) {
	t.Helper()
	c = newCluster(t)
	ldr, flrs = c.ensureLaunch(n)
	return
}

var clusters = make(map[string]int) // map[testname]numClusters

func newCluster(t *testing.T) *cluster {
	heartbeatTimeout := 1000 * time.Millisecond
	var checkLeak func()
	var testTimeout *time.Timer
	clusters[t.Name()]++
	if clusters[t.Name()] == 1 { // first cluster in test, initialize network and checks
		debug()
		debug("-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8-->8--")
		debug()
		tdebug(t.Name(), "--------------------------")
		network = fnet.New()
		checkLeak = leaktest.Check(t)
		testTimeout = time.AfterFunc(time.Minute, func() {
			fmt.Printf("test %s timed out; failing...", t.Name())
			buf := make([]byte, 1024)
			for {
				n := runtime.Stack(buf, true)
				if n < len(buf) {
					buf = buf[:n]
					fmt.Println(string(buf))
					return
				}
				buf = make([]byte, 2*len(buf))
			}
		})
	}
	clusterID++
	c := &cluster{
		T:                t,
		id:               clusterID,
		port:             8888 + int(clusterID),
		checkLeak:        checkLeak,
		testTimeout:      testTimeout,
		rr:               make(map[uint64]*Raft),
		ports:            make(map[uint64]int),
		storage:          make(map[uint64]*Storage),
		serveErr:         make(map[uint64]chan error),
		heartbeatTimeout: heartbeatTimeout,
		longTimeout:      5 * time.Second,
		commitTimeout:    5 * time.Millisecond,
		events: &events{
			observers:   make(map[int]observer),
			states:      make(map[uint64]State),
			ldrs:        make(map[uint64]uint64),
			commitReady: make(map[uint64]bool),
		},
	}
	c.opt = Options{
		HeartbeatTimeout: heartbeatTimeout,
		QuorumWait:       heartbeatTimeout,
		PromoteThreshold: heartbeatTimeout,
		Trace:            c.events.trace(),
	}
	c.storeOpt = DefaultStorageOptions()
	c.storeOpt.LogSegmentSize = 4 * 1024
	return c
}

var clusterID uint64 = 0
var network = fnet.New()

type cluster struct {
	*testing.T
	id               uint64
	port             int
	checkLeak        func()
	testTimeout      *time.Timer
	rr               map[uint64]*Raft
	ports            map[uint64]int
	storage          map[uint64]*Storage
	serverErrMu      sync.RWMutex
	serveErr         map[uint64]chan error
	heartbeatTimeout time.Duration
	longTimeout      time.Duration
	commitTimeout    time.Duration
	opt              Options
	storeOpt         StorageOptions
	resolverMu       sync.RWMutex
	*events
}

func (c *cluster) LookupID(id uint64) (addr string, err error) {
	c.resolverMu.RLock()
	defer c.resolverMu.RUnlock()
	return c.id2Addr(id), nil
}

// if last param is error, call t.Fatal
func (c *cluster) ensure(v ...interface{}) {
	c.Helper()
	if len(v) > 0 {
		if err, ok := v[len(v)-1].(error); ok {
			c.Fatal(err)
		}
	}
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

func (c *cluster) launch(n int, bootstrap bool) map[uint64]*Raft {
	c.Helper()
	tdebug("launch:", n, "bootstrap:", bootstrap)
	nodes := make(map[uint64]Node, n)
	for i := 1; i <= n; i++ {
		id := uint64(i + len(c.rr))
		nodes[id] = Node{ID: id, Addr: c.id2Addr(id), Voter: true}
	}

	launched := make(map[uint64]*Raft)
	for _, node := range nodes {
		tempDir, err := ioutil.TempDir(tempDir, "storage")
		if err != nil {
			c.Fatal(err)
		}
		storage, err := OpenStorage(tempDir, c.storeOpt)
		if err != nil {
			c.Fatalf("openStorage: %v", err)
		}
		if err = storage.SetIdentity(c.id, node.ID); err != nil {
			c.Fatal(err)
		}
		if bootstrap {
			if err := bootstrapStorage(storage, nodes); err != nil {
				c.Fatalf("Storage.bootstrap failed: %v", err)
			}
		}
		fsm := &fsmMock{id: node.ID, changed: c.events.onFMSChanged}
		r, err := New(c.opt, fsm, storage)
		if err != nil {
			c.Fatal(err)
		}
		launched[r.NID()] = r
		c.rr[node.ID] = r
		c.storage[node.ID] = storage
		c.serverErrMu.Lock()
		c.serveErr[r.NID()] = make(chan error, 1)
		c.serverErrMu.Unlock()
	}
	for _, r := range launched {
		c.serve(r)
	}
	return launched
}

func (c *cluster) serve(r *Raft) {
	c.Helper()
	// switch to fake transport
	host := network.Host(id2Host(r.NID()))
	r.dialFn = host.DialTimeout

	l, err := host.Listen("tcp", c.id2Addr(r.NID()))
	if err != nil {
		c.Fatalf("raft.listen failed: %v", err)
	}
	c.eventMu.Lock()
	c.states[r.nid] = Follower
	c.ldrs[r.nid] = 0
	c.eventMu.Unlock()
	go func() {
		err := r.Serve(l)
		c.serverErrMu.RLock()
		c.serveErr[r.NID()] <- err
		c.serverErrMu.RUnlock()
	}()
}

func (c *cluster) ensureLaunch(n int) (ldr *Raft, flrs []*Raft) {
	c.Helper()
	c.launch(n, true)
	ldr = c.waitForHealthy()
	flrs = c.followers()
	return
}

func (c *cluster) shutdownErr(ok bool, rr ...*Raft) {
	c.Helper()
	checkLeak := false
	if len(rr) == 0 {
		tdebug("shutting down cluster")
		checkLeak = true
		rr = c.exclude()
	}
	for _, r := range rr {
		tdebug("shutting down", host(r))
		r.Shutdown()
	}
	for _, r := range rr {
		<-r.Shutdown()
		c.serverErrMu.RLock()
		ch := c.serveErr[r.NID()]
		c.serverErrMu.RUnlock()
		got := <-ch
		ch <- ErrServerClosed
		if ok != (got == ErrServerClosed) {
			c.Errorf("M%d.shutdown: got %v, want ErrServerClosed=%v", r.NID(), got, ok)
		}
	}
	if c.Failed() {
		c.FailNow()
	}
	if checkLeak && c.checkLeak != nil {
		if c.testTimeout != nil {
			c.testTimeout.Stop()
		}
		c.checkLeak()
		c.checkLeak = nil
	}
}

func (c *cluster) shutdown(rr ...*Raft) {
	c.Helper()
	c.shutdownErr(true, rr...)
}

func (c *cluster) restart(r *Raft) *Raft {
	c.Helper()
	c.shutdown(r)

	newFSM := &fsmMock{id: r.NID(), changed: c.events.onFMSChanged}
	storage := c.storage[r.NID()]
	newr, err := New(c.opt, newFSM, storage)
	if err != nil {
		c.Fatal(err)
	}
	c.rr[r.NID()] = newr
	c.serverErrMu.Lock()
	c.serveErr[r.NID()] = make(chan error, 1)
	c.serverErrMu.Unlock()
	tdebug("restarting", host(r))
	c.serve(newr)
	return newr
}

func (c *cluster) waitForStability(rr ...*Raft) {
	c.Helper()
	tdebug("waitForStability:", hosts(rr))
	stateChanged := c.registerFor(stateChanged, rr...)
	defer c.unregister(stateChanged)
	electionStarted := c.registerFor(electionStarted, rr...)
	defer c.unregister(electionStarted)

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
	c.eventMu.RLock()
	defer c.eventMu.RUnlock()
	for _, r := range rr {
		if c.states[r.nid] == state {
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
	c.waitForLeader()
	ldr := c.waitForFollowers()
	return ldr
}

func (c *cluster) ensureLeader(leader uint64) {
	c.Helper()
	for _, r := range c.rr {
		if got := r.Info().Leader(); got != leader {
			c.Fatalf("leader of M%d: got M%d, want M%d", r.NID(), got, leader)
		}
	}
}

// wait until all followers follow the leader
func (c *cluster) waitForFollowers() *Raft {
	c.Helper()
	tdebug("waitForFollowers")

	var ldr uint64
	condition := func(e *event) bool {
		c.eventMu.RLock()
		defer c.eventMu.RUnlock()
		ldr = 0
		for _, v := range c.ldrs {
			if v == 0 {
				return false
			}
			if ldr == 0 {
				ldr = v
			} else if ldr != v {
				return false
			}
		}
		return true
	}
	leaderChanged := c.registerFor(leaderChanged)
	defer c.unregister(leaderChanged)
	if !leaderChanged.waitFor(condition, c.longTimeout) {
		c.eventMu.RLock()
		for id, ldr := range c.ldrs {
			c.Logf("leader of M%d: M%d", id, ldr)
		}
		c.eventMu.RUnlock()
		c.Fatalf("waitForFollowers timeout")
	}
	return c.rr[ldr]
}

// wait until state is one of given states
func (c *cluster) waitForState(r *Raft, timeout time.Duration, states ...State) {
	c.Helper()
	tdebug("waitForState:", host(r), timeout, states)
	condition := func(e *event) bool {
		got := r.Info().State()
		for _, want := range states {
			if got == want {
				return true
			}
		}
		return false
	}
	stateChanged := c.registerFor(stateChanged)
	defer c.unregister(stateChanged)
	if !stateChanged.waitFor(condition, timeout) {
		c.Fatalf("waitForState(M%d, %v) timeout", r.NID(), states)
	}
}

func (c *cluster) waitForLeader(rr ...*Raft) *Raft {
	c.Helper()
	tdebug("waitForLeader:", hosts(rr))
	if len(rr) == 0 {
		rr = c.exclude()
	}
	var ldr *Raft
	condition := func(e *event) bool {
		c.eventMu.RLock()
		defer c.eventMu.RUnlock()
		for _, r := range rr {
			if c.states[r.nid] == Leader {
				ldr = r
				return true
			}
		}
		return false
	}
	stateChanged := c.registerFor(stateChanged)
	defer c.unregister(stateChanged)
	if !stateChanged.waitFor(condition, c.longTimeout) {
		c.Fatalf("waitForLeader: timeout")
	}
	return ldr
}

func (c *cluster) waitCommitReady(ldr *Raft) {
	c.Helper()
	tdebug("waitCommitReady:", host(ldr))
	condition := func(e *event) bool {
		c.eventMu.RLock()
		defer c.eventMu.RUnlock()
		return c.commitReady[ldr.nid]
	}
	commitReady := c.registerFor(commitReady)
	defer c.unregister(commitReady)
	if !commitReady.waitFor(condition, c.longTimeout) {
		c.Fatalf("waitCommitReady: timeout")
	}
}

func (c *cluster) waitFSMLen(fsmLen uint64, rr ...*Raft) {
	c.Helper()
	tdebug("waitFSMLen:", fsmLen, hosts(rr))
	if len(rr) == 0 {
		rr = c.exclude()
	}
	condition := func(e *event) bool {
		for _, r := range rr {
			if got := fsm(r).len(); got != fsmLen {
				return false
			}
		}
		return true
	}
	fsmChanged := c.registerFor(fsmChanged, rr...)
	defer c.unregister(fsmChanged)
	if !fsmChanged.waitFor(condition, c.longTimeout) {
		c.Logf("fsmLen: want %d", fsmLen)
		for _, r := range rr {
			c.Logf("M%d got %d", r.NID(), fsm(r).len())
		}
		c.Fatalf("waitFSMLen(%d) timeout", fsmLen)
	}
}

func (c *cluster) ensureFSMLen(fsmLen uint64, rr ...*Raft) {
	c.Helper()
	tdebug("ensureFSMLen:", fsmLen, hosts(rr))
	if len(rr) == 0 {
		rr = c.exclude()
	}
	for _, r := range rr {
		if got := fsm(r).len(); got != fsmLen {
			c.Fatalf("ensureFSMLen(M%d): got %d, want %d", r.NID(), got, fsmLen)
		}
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

func (c *cluster) waitForCommitted(index uint64, rr ...*Raft) {
	c.Helper()
	tdebug("waitForCommitted: index:", index, hosts(rr))
	if len(rr) == 0 {
		rr = c.exclude()
	}
	log := false
	condition := func() bool {
		for _, r := range rr {
			info := r.Info()
			if info.Committed() < index {
				if log {
					c.Logf("waitForCommitted: M%d lastLogIndex:%d committed:%d", r.NID(), info.LastLogIndex(), info.Committed())
				}
				return false
			}
		}
		return true
	}
	if !waitForCondition(condition, c.commitTimeout, c.longTimeout) {
		log = true
		condition()
		c.Fatalf("waitForCommitted(%d): timeout", index)
	}
}

func (c *cluster) waitCatchup(rr ...*Raft) {
	c.Helper()
	tdebug("waitCatchup:", hosts(rr))
	leaders := c.getInState(Leader)
	if len(leaders) != 1 {
		c.Fatalf("leaders: got %d, want 1", len(leaders))
	}
	if len(rr) == 0 {
		rr = c.exclude(leaders[0])
	}
	ldr := leaders[0].Info()
	log := false
	condition := func() bool {
		for _, r := range rr {
			info := r.Info()
			if info.LastLogIndex() < ldr.LastLogIndex() ||
				info.Committed() < ldr.Committed() {
				if log {
					c.Logf("waitCatchup: M%d lastLogIndex:%d committed:%d", r.NID(), info.LastLogIndex(), info.Committed())
				}
				return false
			}
		}
		return true
	}
	if !waitForCondition(condition, c.commitTimeout, c.longTimeout) {
		c.Logf("waitCatchup: ldr M%d lastLogIndex:%d committed:%d", ldr.NID(), ldr.LastLogIndex(), ldr.Committed())
		log = true
		condition()
		c.Fatal("waitCatchup: timeout")
	}
}

func (c *cluster) waitUnreachableDetected(ldr, failed *Raft) (since time.Time, err error) {
	c.Helper()
	tdebug("waitUnreachableDetected: ldr:", host(ldr), "failed:", host(failed))
	condition := func(e *event) bool {
		flr := ldr.Info().Followers()[failed.NID()]
		since, err = flr.Unreachable, flr.Err
		return !flr.Unreachable.IsZero()
	}
	unreachable := c.registerFor(unreachable, ldr)
	defer c.unregister(unreachable)
	if !unreachable.waitFor(condition, c.longTimeout) {
		c.Fatalf("waitUnreachableDetected: ldr M%d failed to detect M%d is unreachable", ldr.NID(), failed.NID())
	}
	return
}

func (c *cluster) waitReachableDetected(ldr, failed *Raft) {
	c.Helper()
	tdebug("waitReachableDetected: ldr:", host(ldr), "failed:", host(failed))
	condition := func(e *event) bool {
		return ldr.Info().Followers()[failed.NID()].Unreachable.IsZero()
	}
	unreachable := c.registerFor(unreachable, ldr)
	defer c.unregister(unreachable)
	if !unreachable.waitFor(condition, c.longTimeout) {
		c.Fatalf("waitReachableDetected: ldr M%d failed to detect M%d is reachable", ldr.NID(), failed.NID())
	}
}

func (c *cluster) waitTaskDone(t Task, timeout time.Duration, want error) interface{} {
	c.Helper()

	select {
	case <-t.Done():
	case <-time.After(timeout):
		c.Fatal("transferLeadership: timeout")
	}

	// reply must be ErrServerClosed
	if t.Err() != want {
		c.Fatalf("task.Err: got %v, want %v", t.Err(), want)
	}

	return t.Result()
}

func (c *cluster) sendUpdates(r *Raft, from, to int) Task {
	tdebug("sendUpdates:", host(r), from, "to", to)
	var t FSMTask
	for i := from; i <= to; i++ {
		t = UpdateFSM([]byte(fmt.Sprintf("update:%d", i)))
		r.FSMTasks() <- t
	}
	return t
}

func (c *cluster) waitBarrier(r *Raft, timeout time.Duration) {
	c.Helper()
	if _, err := waitFSMTask(r, BarrierFSM(), timeout); err != nil {
		c.Fatalf("Barrer(M%d): timeout", r.NID())
	}
}

func (c *cluster) takeSnapshot(r *Raft, threshold uint64, want error) {
	c.Helper()
	tdebug("takeSnapshot:", host(r), "threshold:", threshold, "want:", want)
	takeSnap := TakeSnapshot(threshold)
	r.Tasks() <- takeSnap
	<-takeSnap.Done()
	if takeSnap.Err() != want {
		c.Fatalf("takeSnapshot(M%d).err: got %v, want %v", r.nid, takeSnap.Err(), want)
	}
}

func (c *cluster) disconnect(rr ...*Raft) {
	tdebug("disconnect:", hosts(rr))
	if len(rr) == 0 {
		network.SetFirewall(fnet.AllowSelf)
		return
	}
	var hosts []string
	for _, r := range rr {
		hosts = append(hosts, id2Host(r.NID()))
	}
	network.SetFirewall(fnet.Split(hosts, fnet.AllowAll))
}

func (c *cluster) connect() {
	tdebug("reconnecting")
	network.SetFirewall(fnet.AllowAll)
}

func (c *cluster) snaps(r *Raft) []uint64 {
	c.Helper()
	snaps, err := findSnapshots(r.snaps.dir)
	if err != nil {
		c.Fatal(err)
	}
	return snaps
}

// ---------------------------------------------

func fsm(r *Raft) *fsmMock {
	return r.FSM().(*fsmMock)
}

func waitTask(r *Raft, t Task, timeout time.Duration) (interface{}, error) {
	tdebug("waitTask:", host(r), t, timeout)
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

func waitBootstrap(r *Raft, c Config, timeout time.Duration) error {
	tdebug("waitBootstrap:", host(r), c, timeout)
	_, err := waitTask(r, ChangeConfig(c), timeout)
	return err
}

func addNonvoter(ldr *Raft, id uint64, addr string, promote bool) Task {
	tdebug("addNonvoter:", host(ldr), id2Host(id), addr, promote)
	newConf := ldr.Info().Configs().Latest
	action := None
	if promote {
		action = Promote
	}
	newConf.Nodes[id] = Node{ID: id, Addr: addr, Action: action}
	t := ChangeConfig(newConf)
	ldr.Tasks() <- t
	return t
}

func waitAddNonvoter(ldr *Raft, id uint64, addr string, promote bool) error {
	tdebug("waitAddNonvoter:", host(ldr), id2Host(id), addr, promote)
	newConf := ldr.Info().Configs().Latest
	action := None
	if promote {
		action = Promote
	}
	newConf.Nodes[id] = Node{ID: id, Addr: addr, Action: action}
	t := ChangeConfig(newConf)
	if _, err := waitTask(ldr, t, 0); err != nil {
		return err
	}
	return nil
}

// use zero timeout, to wait till reply received
func waitFSMTask(r *Raft, t FSMTask, timeout time.Duration) (fsmReply, error) {
	tdebug("waitNewEntry:", host(r), t, timeout)
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	select {
	case r.FSMTasks() <- t:
		break
	case <-timer:
		return fsmReply{}, errors.New("waitFSMTask: submit timeout")
	}
	select {
	case <-t.Done():
		if t.Err() != nil {
			return fsmReply{}, t.Err()
		}
		result := fsmReply{}
		if t.Result() != nil {
			result = t.Result().(fsmReply)
		}
		return result, nil
	case <-timer:
		return fsmReply{}, errors.New("waitNewEntry: result timeout")
	}
}

func waitUpdate(r *Raft, cmd string, timeout time.Duration) (fsmReply, error) {
	return waitFSMTask(r, UpdateFSM([]byte(cmd)), timeout)
}

func waitRead(r *Raft, read string, timeout time.Duration) (fsmReply, error) {
	return waitFSMTask(r, ReadFSM([]byte(read)), timeout)
}

func requestVote(from, to *Raft) (granted bool, err error) {
	fn := func(r *Raft) {
		req := &voteReq{
			req:          req{r.term, r.nid},
			lastLogIndex: r.lastLogIndex,
			lastLogTerm:  r.lastLogTerm,
		}
		pool := from.getConnPool(to.nid)
		resp := &timeoutNowResp{}
		err = pool.doRPC(req, resp)
		granted = resp.getResult() == success
	}
	if from.isClosed() {
		fn(from)
	} else {
		ierr := from.inspect(fn)
		if err == nil {
			err = ierr
		}
	}
	return
}

func bootstrapStorage(store *Storage, nodes map[uint64]Node) error {
	return store.bootstrap(Config{Nodes: nodes, Index: 1, Term: 1})
}

// events ---------------------------------------------

type eventType int

const (
	fsmChanged eventType = iota
	stateChanged
	leaderChanged
	electionStarted
	electionAborted
	commitReady
	configChanged
	configCommitted
	configReverted
	unreachable
	quorumUnreachable
	roundFinished
	logCompacted
	configActionStarted
	shuttingDown

	configRelated
)

type event struct {
	src uint64
	typ eventType

	fsmLen     uint64
	state      State
	leader     uint64
	configs    Configs
	target     uint64
	since      time.Time
	err        error
	msgType    string
	round      Round
	action     ConfigAction
	numRounds  uint64
	firstIndex uint64
}

func (e event) matches(typ eventType, rr ...*Raft) bool {
	if typ == configRelated {
		switch e.typ {
		case configChanged, configCommitted, configReverted:
		default:
			return false
		}
	} else if typ != e.typ {
		return false
	}

	if len(rr) > 0 {
		for _, r := range rr {
			if e.src == r.NID() {
				return true
			}
		}
		return false
	}
	return true
}

type observer struct {
	filter func(event) bool
	ch     chan event
	id     int
}

func (ob observer) waitForEvent(timeout time.Duration) (event, error) {
	select {
	case e := <-ob.ch:
		return e, nil
	case <-timeAfter(timeout):
		return event{}, errors.New("waitForEvent: timeout")
	}
}

func (ob observer) waitFor(condition func(*event) bool, timeout time.Duration) bool {
	timeoutCh := timeAfter(timeout)
	if condition(nil) {
		return true
	}
	for {
		select {
		case e := <-ob.ch:
			if condition(&e) {
				return true
			}
		case <-timeoutCh:
			return false
		}
	}
}

type events struct {
	observersMu sync.RWMutex
	observerID  int
	observers   map[int]observer

	eventMu     sync.RWMutex
	states      map[uint64]State
	ldrs        map[uint64]uint64
	commitReady map[uint64]bool
}

func (ee *events) register(filter func(event) bool) observer {
	ee.observersMu.Lock()
	defer ee.observersMu.Unlock()
	ob := observer{
		filter: filter,
		ch:     make(chan event, 1000),
		id:     ee.observerID,
	}
	ee.observers[ob.id] = ob
	ee.observerID++
	return ob
}

func (ee *events) registerFor(typ eventType, rr ...*Raft) observer {
	return ee.register(func(e event) bool {
		return e.matches(typ, rr...)
	})
}

func (ee *events) unregister(ob observer) {
	ee.observersMu.Lock()
	defer ee.observersMu.Unlock()
	delete(ee.observers, ob.id)
}

func (ee *events) sendEvent(e event) {
	ee.observersMu.RLock()
	defer ee.observersMu.RUnlock()
	for _, ob := range ee.observers {
		if ob.filter(e) {
			ob.ch <- e
		}
	}
}

func (ee *events) onFMSChanged(id uint64, len uint64) {
	ee.sendEvent(event{
		src:    id,
		typ:    fsmChanged,
		fsmLen: len,
	})
}

func (ee *events) trace() (trace Trace) {
	trace.ShuttingDown = func(info Info, reason error) {
		ee.sendEvent(event{
			src: info.NID(),
			typ: shuttingDown,
			err: reason,
		})
	}
	trace.StateChanged = func(info Info) {
		ee.eventMu.Lock()
		ee.states[info.NID()] = info.State()
		ee.commitReady[info.NID()] = false
		ee.eventMu.Unlock()
		ee.sendEvent(event{
			src:   info.NID(),
			typ:   stateChanged,
			state: info.State(),
		})
	}
	trace.LeaderChanged = func(info Info) {
		ee.eventMu.Lock()
		ee.ldrs[info.NID()] = info.Leader()
		ee.eventMu.Unlock()
		ee.sendEvent(event{
			src:    info.NID(),
			typ:    leaderChanged,
			leader: info.Leader(),
		})
	}
	trace.ElectionStarted = func(info Info) {
		ee.sendEvent(event{
			src: info.NID(),
			typ: electionStarted,
		})
	}
	trace.ElectionAborted = func(info Info, reason string) {
		ee.sendEvent(event{
			src: info.NID(),
			typ: electionAborted,
		})
	}
	trace.CommitReady = func(info Info) {
		ee.eventMu.Lock()
		ee.commitReady[info.NID()] = true
		ee.eventMu.Unlock()
		ee.sendEvent(event{
			src: info.NID(),
			typ: commitReady,
		})
	}
	trace.ConfigChanged = func(info Info) {
		ee.sendEvent(event{
			src:     info.NID(),
			typ:     configChanged,
			configs: info.Configs(),
		})
	}

	trace.ConfigCommitted = func(info Info) {
		ee.sendEvent(event{
			src:     info.NID(),
			typ:     configCommitted,
			configs: info.Configs(),
		})
	}

	trace.ConfigReverted = func(info Info) {
		ee.sendEvent(event{
			src:     info.NID(),
			typ:     configReverted,
			configs: info.Configs(),
		})
	}

	trace.Unreachable = func(info Info, id uint64, since time.Time, err error) {
		ee.sendEvent(event{
			src:    info.NID(),
			typ:    unreachable,
			target: id,
			since:  since,
			err:    err,
		})
	}

	trace.QuorumUnreachable = func(info Info, since time.Time) {
		ee.sendEvent(event{
			src:   info.NID(),
			typ:   quorumUnreachable,
			since: since,
		})
	}

	trace.RoundCompleted = func(info Info, id uint64, r Round) {
		ee.sendEvent(event{
			src:    info.NID(),
			typ:    roundFinished,
			target: id,
			round:  r,
		})
	}

	trace.LogCompacted = func(info Info) {
		ee.sendEvent(event{
			src:        info.NID(),
			typ:        logCompacted,
			firstIndex: info.FirstLogIndex(),
		})
	}

	trace.ConfigActionStarted = func(info Info, id uint64, action ConfigAction) {
		ee.sendEvent(event{
			src:       info.NID(),
			typ:       configActionStarted,
			target:    id,
			action:    action,
			numRounds: info.Followers()[id].Round,
		})
	}
	return
}

// fsmMock ---------------------------------------------

var errNoCommands = errors.New("no commands")
var errNoCommandAt = errors.New("no command at index")

type fsmMock struct {
	id      uint64
	mu      sync.RWMutex
	cmds    []string
	changed func(id uint64, len uint64)
}

var _ FSM = (*fsmMock)(nil)

type fsmReply struct {
	msg   string
	index int
}

func (fsm *fsmMock) Update(cmd []byte) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	s := string(cmd)
	fsm.cmds = append(fsm.cmds, s)
	if fsm.changed != nil {
		fsm.changed(fsm.id, uint64(len(fsm.cmds)))
	}
	return fsmReply{s, len(fsm.cmds)}
}

func (fsm *fsmMock) Read(cmd []byte) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	s := string(cmd)
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

func tdebug(args ...interface{}) {
	prefix := "[testing]-----------------------"
	debug(append([]interface{}{prefix}, args...)...)
}

func hosts(rr []*Raft) string {
	if len(rr) == 0 {
		return "all"
	}
	var buf bytes.Buffer
	for i, r := range rr {
		if i != 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(id2Host(r.NID()))
	}
	return buf.String()
}

func timeAfter(timeout time.Duration) <-chan time.Time {
	if timeout == 0 {
		return nil
	}
	return time.After(timeout)
}

func wgChannel(wg *sync.WaitGroup) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func waitWG(wg *sync.WaitGroup, timeout time.Duration) error {
	select {
	case <-wgChannel(wg):
		return nil
	case <-timeAfter(timeout):
		return errors.New("waitWG: timeout")
	}
}

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
