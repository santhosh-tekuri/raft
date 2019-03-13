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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/santhosh-tekuri/fnet"
)

func TestRaft(t *testing.T) {
	//t.Run("voting", test_voting)
	t.Run("shutdown", test_shutdown)
	t.Run("bootstrap", test_bootstrap)
	t.Run("singleNode", test_singleNode)
	t.Run("tripleNode", test_tripleNode)
	t.Run("leader", func(t *testing.T) {
		t.Run("stepDown", test_leader_stepDown)
		t.Run("quorumWait", func(t *testing.T) {
			t.Run("unreachable", test_leader_quorumWait_unreachable)
			t.Run("reachable", test_leader_quorumWait_reachable)
		})
	})
	t.Run("behindFollower", test_behindFollower)
	t.Run("update", func(t *testing.T) {
		t.Run("nonLeader", test_update_nonLeader)
		t.Run("concurrent", test_update_concurrent)
	})
	t.Run("barrier", test_barrier)
	t.Run("query", test_query)
	t.Run("takeSnapshot", func(t *testing.T) {
		t.Run("emptyFSM", test_takeSnapshot_emptyFSM)
		t.Run("thresholdNotReached", test_takeSnapshot_thresholdNotReached)
		//todo: test ErrSnapshotInProgress
		t.Run("restartSendUpdates", test_takeSnapshot_restartSendUpdates)
	})
	t.Run("sendSnapshot", func(t *testing.T) {
		t.Run("case1", func(t *testing.T) {
			test_sendSnapshot_case(t, false)
		})
		t.Run("case2", func(t *testing.T) {
			test_sendSnapshot_case(t, true)
		})
	})
	t.Run("changeConfig", func(t *testing.T) {
		t.Run("validations", test_changeConfig_validations)
		t.Run("committedByAll", test_changeConfig_committedByAll)
	})
	t.Run("nonvoter", func(t *testing.T) {
		t.Run("catchesUp_followsLeader", test_nonvoter_catchesUp_followsLeader)
		t.Run("reconnects_catchesUp", test_nonvoter_reconnects_catchesUp)
		t.Run("leaderChanged_followsNewLeader", test_nonvoter_leaderChanged_followsNewLeader)
	})
	t.Run("promote", func(t *testing.T) {
		t.Run("newNode", func(t *testing.T) {
			t.Run("singleRound", test_promote_newNode_singleRound)
			t.Run("uptodateButConfigChangeInProgress", test_promote_newNode_uptodateButConfigChangeInProgress)
			t.Run("multipleRound", test_promote_newNode_multipleRounds)
		})
		t.Run("existingNode", func(t *testing.T) {
			t.Run("notUpToDate", test_promote_existingNode_notUpToDate)
			t.Run("upToDate", test_promote_existingNode_upToDate)
		})
	})
	t.Run("transferLeadership", func(t *testing.T) {
		t.Run("singleVoter", test_transferLeadership_singleVoter)
		t.Run("targetNotReady", func(t *testing.T) {
			test_transferLeadership_fiveNodes(t, false)
		})
		t.Run("targetReady", func(t *testing.T) {
			test_transferLeadership_fiveNodes(t, true)
		})
		t.Run("rejectAnotherTransferRequest", test_transferLeadership_rejectAnotherTransferRequest)
		t.Run("rejectLogUpdateTasks", test_transferLeadership_rejectLogUpdateTasks)
		t.Run("quorumUnreachable", test_transferLeadership_quorumUnreachable)
		t.Run("newTermDetected", test_transferLeadership_newTermDetected)
		// todo: add test for timeoutError
	})
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

func host(r *Raft) string {
	return id2Host(r.ID())
}

func id2Host(id uint64) string {
	return fmt.Sprintf("M%d", id)
}

func id2Addr(id uint64) string {
	return id2Host(id) + ":8888"
}

func (c *cluster) inmemStorage(r *Raft) *inmemStorage {
	return r.storage.log.(*inmemStorage)
}

func launchCluster(t *testing.T, n int) (c *cluster, ldr *Raft, followers []*Raft) {
	t.Helper()
	c = newCluster(t)
	ldr, followers = c.ensureLaunch(n)
	return
}

func newCluster(t *testing.T) *cluster {
	Debug(t.Name(), "--------------------------")
	heartbeatTimeout := 50 * time.Millisecond
	testTimeout := time.AfterFunc(time.Minute, func() {
		t.Error("test timed out; failing...")
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
	c := &cluster{
		T:                t,
		checkLeak:        leaktest.Check(t),
		testTimeout:      testTimeout,
		network:          fnet.New(),
		rr:               make(map[uint64]*Raft),
		storage:          make(map[uint64]Storage),
		heartbeatTimeout: heartbeatTimeout,
		longTimeout:      5 * time.Second,
		commitTimeout:    5 * time.Millisecond,
		events:           &events{observers: make(map[int]observer)},
	}
	c.opt = Options{
		HeartbeatTimeout: heartbeatTimeout,
		QuorumWait:       heartbeatTimeout,
		PromoteThreshold: heartbeatTimeout,
		Trace:            c.events.trace(),
	}
	return c
}

type cluster struct {
	*testing.T
	checkLeak        func()
	testTimeout      *time.Timer
	rr               map[uint64]*Raft
	storage          map[uint64]Storage
	network          *fnet.Network
	heartbeatTimeout time.Duration
	longTimeout      time.Duration
	commitTimeout    time.Duration
	opt              Options
	*events
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
		nodes[id] = Node{ID: id, Addr: id2Addr(id), Voter: true}
	}

	launched := make(map[uint64]*Raft)
	for _, node := range nodes {
		s := new(inmemStorage)
		storage := Storage{Vars: s, Log: s, Snapshots: s}
		if bootstrap {
			if err := BootstrapStorage(storage, nodes); err != nil {
				c.Fatalf("Storage.bootstrap failed: %v", err)
			}
		}
		fsm := &fsmMock{id: node.ID, changed: c.events.onFMSChanged}
		r, err := New(node.ID, c.opt, fsm, storage)
		if err != nil {
			c.Fatal(err)
		}
		launched[r.ID()] = r
		c.rr[node.ID] = r
		c.storage[node.ID] = storage

		// switch to fake transport
		host := c.network.Host(id2Host(r.ID()))
		r.dialFn = host.DialTimeout

		l, err := host.Listen("tcp", node.Addr)
		if err != nil {
			c.Fatalf("raft.listen failed: %v", err)
		}
		go func() { _ = r.Serve(l) }()
	}
	return launched
}

func (c *cluster) ensureLaunch(n int) (ldr *Raft, followers []*Raft) {
	c.Helper()
	c.launch(n, true)
	ldr = c.waitForHealthy()
	c.ensureLeader(ldr.ID())
	followers = c.followers()
	return
}

func (c *cluster) shutdown(rr ...*Raft) {
	c.Helper()
	checkLeak := false
	if len(rr) == 0 {
		tdebug("shutting down cluster")
		checkLeak = true
		rr = c.exclude()
	}
	for _, r := range rr {
		tdebug("shutting down", host(r))
		r.Shutdown().Wait()
		tdebug(host(r), "is shutdown")
	}
	if checkLeak && c.checkLeak != nil {
		c.testTimeout.Stop()
		c.checkLeak()
		c.checkLeak = nil
	}
}

func (c *cluster) restart(r *Raft) *Raft {
	c.Helper()
	c.shutdown(r)

	newFSM := &fsmMock{id: r.ID(), changed: c.events.onFMSChanged}
	storage := c.storage[r.ID()]
	newr, err := New(r.ID(), c.opt, newFSM, storage)
	if err != nil {
		c.Fatal(err)
	}

	h := c.network.Host(id2Host(r.ID()))
	newr.dialFn = h.DialTimeout

	l, err := h.Listen("tcp", id2Addr(r.ID()))
	if err != nil {
		c.Fatal(err)
	}
	c.rr[r.ID()] = newr
	tdebug("restarting", host(r))
	go newr.Serve(l)
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

func (c *cluster) ensureLeader(leader uint64) {
	c.Helper()
	for _, r := range c.rr {
		if got := r.Info().Leader(); got != leader {
			c.Fatalf("leader of M%d: got M%d, want M%d", r.ID(), got, leader)
		}
	}
}

// wait until state is one of given states
func (c *cluster) waitForState(r *Raft, timeout time.Duration, states ...State) {
	c.Helper()
	tdebug("waitForState:", host(r), timeout, states)
	condition := func() bool {
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
		c.Fatalf("waitForState(M%d, %v) timeout", r.ID(), states)
	}
}

func (c *cluster) waitForLeader(rr ...*Raft) *Raft {
	c.Helper()
	tdebug("waitForLeader:", hosts(rr))
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
	stateChanged := c.registerFor(stateChanged)
	defer c.unregister(stateChanged)
	if !stateChanged.waitFor(condition, c.longTimeout) {
		c.Fatalf("waitForLeader: timeout")
	}
	return c.getInState(Leader, rr...)[0]
}

func (c *cluster) waitFSMLen(fsmLen uint64, rr ...*Raft) {
	c.Helper()
	tdebug("waitFSMLen:", fsmLen, hosts(rr))
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
	fsmChanged := c.registerFor(fsmChanged, rr...)
	defer c.unregister(fsmChanged)
	if !fsmChanged.waitFor(condition, c.longTimeout) {
		c.Logf("fsmLen: want %d", fsmLen)
		for _, r := range rr {
			c.Logf("M%d got %d", r.ID(), fsm(r).len())
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
			c.Fatalf("ensureFSMLen(M%d): got %d, want %d", r.ID(), got, fsmLen)
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
					c.Logf("waitCatchup: M%d lastLogIndex:%d committed:%d", r.ID(), info.LastLogIndex(), info.Committed())
				}
				return false
			}
		}
		return true
	}
	if !waitForCondition(condition, c.commitTimeout, c.longTimeout) {
		c.Logf("waitCatchup: ldr M%d lastLogIndex:%d committed:%d", ldr.ID(), ldr.LastLogIndex(), ldr.Committed())
		log = true
		condition()
		c.Fatal("waitCatchup: timeout")
	}
}

func (c *cluster) waitUnreachableDetected(ldr, failed *Raft) {
	c.Helper()
	tdebug("waitUnreachableDetected: ldr:", host(ldr), "failed:", host(failed))
	condition := func() bool {
		return !ldr.Info().Followers()[failed.ID()].Unreachable.IsZero()
	}
	unreachable := c.registerFor(unreachable, ldr)
	defer c.unregister(unreachable)
	if !unreachable.waitFor(condition, c.longTimeout) {
		c.Fatalf("waitUnreachableDetected: ldr M%d failed to detect M%d is unreachable", ldr.ID(), failed.ID())
	}
}

func (c *cluster) sendUpdates(r *Raft, from, to int) Task {
	tdebug("sendUpdates:", host(r), from, "to", to)
	var ne NewEntry
	for i := from; i <= to; i++ {
		ne = UpdateFSM([]byte(fmt.Sprintf("update:%d", i)))
		r.NewEntries() <- ne
	}
	return ne
}

func (c *cluster) waitBarrier(r *Raft, timeout time.Duration) {
	c.Helper()
	if _, err := waitNewEntry(r, BarrierFSM(), timeout); err != nil {
		c.Fatalf("Barrer(M%d): timeout", r.ID())
	}
}

func (c *cluster) takeSnapshot(r *Raft, threshold uint64, want error) {
	c.Helper()
	tdebug("takeSnapshot:", host(r), "threshold:", threshold, "want:", want)
	takeSnap := TakeSnapshot(threshold)
	r.Tasks() <- takeSnap
	<-takeSnap.Done()
	if takeSnap.Err() != want {
		c.Fatalf("takeSnapshot(M%d).err: got %s, want %v", r.id, takeSnap.Err(), want)
	}
}

func (c *cluster) disconnect(rr ...*Raft) {
	tdebug("disconnect:", hosts(rr))
	if len(rr) == 0 {
		c.network.SetFirewall(fnet.AllowSelf)
		return
	}
	var hosts []string
	for _, r := range rr {
		hosts = append(hosts, id2Host(r.ID()))
	}
	c.network.SetFirewall(fnet.Split(hosts, fnet.AllowAll))
}

func (c *cluster) connect() {
	tdebug("reconnecting")
	c.network.SetFirewall(fnet.AllowAll)
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

func waitBootstrap(r *Raft, nodes map[uint64]Node, timeout time.Duration) error {
	_, err := waitTask(r, Bootstrap(nodes), timeout)
	return err
}

func addNonvoter(ldr *Raft, id uint64, addr string, promote bool) Task {
	tdebug("addNonvoter:", host(ldr), id2Host(id), addr, promote)
	newConf := ldr.Info().Configs().Latest
	newConf.Nodes[id] = Node{ID: id, Addr: addr, Promote: promote}
	t := ChangeConfig(newConf)
	ldr.Tasks() <- t
	return t
}

func waitAddNonvoter(ldr *Raft, id uint64, addr string, promote bool) error {
	tdebug("waitAddNonvoter:", host(ldr), id2Host(id), addr, promote)
	newConf := ldr.Info().Configs().Latest
	newConf.Nodes[id] = Node{ID: id, Addr: addr, Promote: promote}
	t := ChangeConfig(newConf)
	if _, err := waitTask(ldr, t, 0); err != nil {
		return err
	}
	return nil
}

// use zero timeout, to wait till reply received
func waitNewEntry(r *Raft, ne NewEntry, timeout time.Duration) (fsmReply, error) {
	tdebug("waitNewEntry:", host(r), ne, timeout)
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
		return fsmReply{}, errors.New("waitNewEntry: result timeout")
	}
}

func waitUpdate(r *Raft, cmd string, timeout time.Duration) (fsmReply, error) {
	return waitNewEntry(r, UpdateFSM([]byte(cmd)), timeout)
}

func waitQuery(r *Raft, query string, timeout time.Duration) (fsmReply, error) {
	return waitNewEntry(r, QueryFSM([]byte(query)), timeout)
}

// events ---------------------------------------------

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
	quorumUnreachable
	roundFinished
	promoting
	sending
	received

	configRelated
)

type event struct {
	src uint64
	typ eventType

	fsmLen    uint64
	state     State
	configs   Configs
	target    uint64
	since     time.Time
	err       error
	msgType   string
	round     Round
	numRounds uint64
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
			if e.src == r.ID() {
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

func (ob observer) waitFor(condition func() bool, timeout time.Duration) bool {
	timeoutCh := timeAfter(timeout)
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

type events struct {
	observersMu sync.RWMutex
	observerID  int
	observers   map[int]observer
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
	trace.Error = func(err error) {
		assert(false, "unexpected error: %v", err)
	}

	trace.StateChanged = func(info Info) {
		ee.sendEvent(event{
			src:   info.ID(),
			typ:   stateChanged,
			state: info.State(),
		})
	}
	trace.ElectionStarted = func(info Info) {
		ee.sendEvent(event{
			src: info.ID(),
			typ: electionStarted,
		})
	}
	trace.ElectionAborted = func(info Info, reason string) {
		ee.sendEvent(event{
			src: info.ID(),
			typ: electionAborted,
		})
	}

	trace.ConfigChanged = func(info Info) {
		ee.sendEvent(event{
			src:     info.ID(),
			typ:     configChanged,
			configs: info.Configs(),
		})
	}

	trace.ConfigCommitted = func(info Info) {
		ee.sendEvent(event{
			src:     info.ID(),
			typ:     configCommitted,
			configs: info.Configs(),
		})
	}

	trace.ConfigReverted = func(info Info) {
		ee.sendEvent(event{
			src:     info.ID(),
			typ:     configReverted,
			configs: info.Configs(),
		})
	}

	trace.Unreachable = func(info Info, id uint64, since time.Time, err error) {
		ee.sendEvent(event{
			src:    info.ID(),
			typ:    unreachable,
			target: id,
			since:  since,
			err:    err,
		})
	}

	trace.QuorumUnreachable = func(info Info, since time.Time) {
		ee.sendEvent(event{
			src:   info.ID(),
			typ:   quorumUnreachable,
			since: since,
		})
	}

	trace.RoundCompleted = func(info Info, id uint64, r Round) {
		ee.sendEvent(event{
			src:    info.ID(),
			typ:    roundFinished,
			target: id,
			round:  r,
		})
	}

	trace.Promoting = func(info Info, id, numRounds uint64) {
		ee.sendEvent(event{
			src:       info.ID(),
			typ:       promoting,
			target:    id,
			numRounds: numRounds,
		})
	}

	trace.sending = func(from, to uint64, state State, msg message) {
		if _, ok := msg.(request); ok && state != Leader {
			str := fmt.Sprintf("M%d %d %s | M%d", from, msg.getTerm(), string(state), to)
			Debug(str, ">>>", msg)
		}
		ee.sendEvent(event{
			src:     from,
			typ:     sending,
			target:  to,
			msgType: fmt.Sprintf("%T", msg),
		})
	}

	trace.received = func(by, from uint64, state State, term uint64, msg message) {
		if state != Leader {
			str := fmt.Sprintf("M%d %d %s |", by, term, string(state))
			if _, ok := msg.(request); ok {
				Debug(str, "<<<", msg)
			} else {
				str = fmt.Sprintf("%s M%d", str, from)
				Debug(str, "<<<", msg)
			}
		}
		ee.sendEvent(event{
			src:     by,
			typ:     received,
			target:  from,
			msgType: fmt.Sprintf("%T", msg),
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

// inmemStorage ------------------------------------------------------------------

var (
	ErrNotFound   = errors.New("not found")
	ErrOutOfRange = errors.New("out of range")
)

type inmemStorage struct {
	muStable      sync.RWMutex
	term          uint64
	vote          uint64
	confCommitted uint64
	confLatest    uint64

	muLog   sync.RWMutex
	entries [][]byte

	snapMeta SnapshotMeta
	snapshot *bytes.Buffer
}

func (s *inmemStorage) GetVote() (term, vote uint64, err error) {
	s.muStable.RLock()
	defer s.muStable.RUnlock()
	return s.term, s.vote, nil
}

func (s *inmemStorage) SetVote(term, vote uint64) error {
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

func (s *inmemStorage) numEntries() int {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	return len(s.entries)
}

func (s *inmemStorage) Count() (uint64, error) {
	return uint64(s.numEntries()), nil
}

func (s *inmemStorage) Empty() (bool, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	return len(s.entries) == 0, nil
}

func (s *inmemStorage) First() ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if len(s.entries) == 0 {
		return nil, ErrNotFound
	}
	return s.entries[0], nil
}

func (s *inmemStorage) Last() ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if len(s.entries) == 0 {
		return nil, ErrNotFound
	}
	return s.entries[len(s.entries)-1], nil
}

func (s *inmemStorage) Get(offset uint64) ([]byte, error) {
	s.muLog.RLock()
	defer s.muLog.RUnlock()
	if offset >= uint64(len(s.entries)) {
		return nil, ErrNotFound
	}
	return s.entries[offset], nil
}

func (s *inmemStorage) Append(entry []byte) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	s.entries = append(s.entries, entry)
	return nil
}

func (s *inmemStorage) DeleteFirst(n uint64) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	if n > uint64(len(s.entries)) {
		return ErrOutOfRange
	}
	s.entries = s.entries[n:]
	return nil
}

func (s *inmemStorage) DeleteLast(n uint64) error {
	s.muLog.Lock()
	defer s.muLog.Unlock()
	if n > uint64(len(s.entries)) {
		return ErrOutOfRange
	}
	s.entries = s.entries[:len(s.entries)-int(n)]
	return nil
}

func (s *inmemStorage) numSnaps() int {
	if s.snapMeta.Index == 0 {
		return 0
	}
	return 1
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
	if index < s.snapMeta.Index {
		panic("i have latest snapshot")
	}
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

func (s *inmemStorage) Meta() (SnapshotMeta, error) {
	return s.snapMeta, nil
}

func (s *inmemStorage) Open() (SnapshotMeta, io.ReadCloser, error) {
	if s.snapMeta.Index == 0 {
		panic("no snapshots")
	}
	return s.snapMeta, ioutil.NopCloser(bytes.NewReader(s.snapshot.Bytes())), nil
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
		buf.WriteString(id2Host(r.ID()))
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
