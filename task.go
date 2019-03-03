package raft

import (
	"errors"
	"time"
)

func (r *Raft) Tasks() chan<- Task {
	return r.taskCh
}

type Task interface {
	Done() <-chan struct{}
	Err() error
	Result() interface{}
	reply(interface{})
}

// ---------------------------------------

type task struct {
	result interface{}
	done   chan struct{}
}

func newTask() *task {
	return &task{done: make(chan struct{})}
}

func (t *task) Done() <-chan struct{} {
	return t.done
}

func (t *task) Err() error {
	if err, ok := t.result.(error); ok {
		return err
	}
	return nil
}

func (t *task) Result() interface{} {
	if _, ok := t.result.(error); ok {
		return nil
	}
	return t.result
}

func (t *task) reply(result interface{}) {
	if t != nil {
		t.result = result
		if t.done != nil {
			close(t.done)
		}
	}
}

// ------------------------------------------------------------------------

type NewEntry struct {
	*task
	*entry
}

func (r *Raft) NewEntries() chan<- NewEntry {
	return r.newEntryCh
}

func newEntry(typ entryType, data []byte) NewEntry {
	return NewEntry{
		task:  newTask(),
		entry: &entry{typ: typ, data: data},
	}
}

func UpdateEntry(data []byte) NewEntry {
	return newEntry(entryUpdate, data)
}

func QueryEntry(data []byte) NewEntry {
	return newEntry(entryQuery, data)
}

// BarrierEntry is used to issue a command that blocks until all preceding
// commands have been applied to the FSM. It can be used to ensure the
// FSM reflects all queued commands.
func BarrierEntry() NewEntry {
	return newEntry(entryBarrier, nil)
}

// ------------------------------------------------------------------------

type bootstrap struct {
	*task
	nodes map[ID]Node
}

func Bootstrap(nodes map[ID]Node) Task {
	return bootstrap{task: newTask(), nodes: nodes}
}

// ------------------------------------------------------------------------

type ReplStatus struct {
	ID          ID        `json:"-"`
	MatchIndex  uint64    `json:"matchIndexes"`
	Unreachable time.Time `json:"unreachable,omitempty"`
}

type json struct {
	ID           ID                `json:"id"`
	Addr         string            `json:"addr"`
	Term         uint64            `json:"term"`
	State        State             `json:"state"`
	Leader       ID                `json:"leader,omitempty"`
	LastLogIndex uint64            `json:"lastLogIndex"`
	LastLogTerm  uint64            `json:"lastLogTerm"`
	Committed    uint64            `json:"committed"`
	LastApplied  uint64            `json:"lastApplied"`
	Configs      Configs           `json:"configs"`
	Replication  map[ID]ReplStatus `json:"replication,omitempty"`
}

type Info interface {
	ID() ID
	Addr() string
	Term() uint64
	State() State
	Leader() ID
	LastLogIndex() uint64
	LastLogTerm() uint64
	Committed() uint64
	LastApplied() uint64
	Configs() Configs
	Replication() map[ID]ReplStatus
	Trace() *Trace
	JSON() interface{}
}

type liveInfo struct {
	r   *Raft
	ldr *leadership
}

func (info liveInfo) ID() ID               { return info.r.id }
func (info liveInfo) Addr() string         { return info.r.addr() }
func (info liveInfo) Term() uint64         { return info.r.term }
func (info liveInfo) State() State         { return info.r.state }
func (info liveInfo) Leader() ID           { return info.r.leader }
func (info liveInfo) LastLogIndex() uint64 { return info.r.lastLogIndex }
func (info liveInfo) LastLogTerm() uint64  { return info.r.lastLogTerm }
func (info liveInfo) Committed() uint64    { return info.r.commitIndex }
func (info liveInfo) LastApplied() uint64  { return info.r.lastApplied }
func (info liveInfo) Configs() Configs     { return info.r.configs.clone() }
func (info liveInfo) Trace() *Trace        { return &info.r.trace }

func (info liveInfo) Replication() map[ID]ReplStatus {
	if info.ldr == nil {
		return nil
	}
	m := make(map[ID]ReplStatus)
	for id, repl := range info.ldr.repls {
		m[id] = ReplStatus{
			MatchIndex:  repl.status.matchIndex,
			Unreachable: repl.status.noContact,
		}
	}
	return m
}

func (info liveInfo) JSON() interface{} {
	return json{
		ID:           info.ID(),
		Addr:         info.Addr(),
		Term:         info.Term(),
		State:        info.State(),
		Leader:       info.Leader(),
		LastLogIndex: info.LastLogIndex(),
		LastLogTerm:  info.LastLogTerm(),
		Committed:    info.Committed(),
		LastApplied:  info.LastApplied(),
		Configs:      info.Configs(),
		Replication:  info.Replication(),
	}
}

type cachedInfo struct {
	json json
}

func (info cachedInfo) ID() ID                         { return info.json.ID }
func (info cachedInfo) Addr() string                   { return info.json.Addr }
func (info cachedInfo) Term() uint64                   { return info.json.Term }
func (info cachedInfo) State() State                   { return info.json.State }
func (info cachedInfo) Leader() ID                     { return info.json.Leader }
func (info cachedInfo) LastLogIndex() uint64           { return info.json.LastLogIndex }
func (info cachedInfo) LastLogTerm() uint64            { return info.json.LastLogTerm }
func (info cachedInfo) Committed() uint64              { return info.json.Committed }
func (info cachedInfo) LastApplied() uint64            { return info.json.LastApplied }
func (info cachedInfo) Configs() Configs               { return info.json.Configs }
func (info cachedInfo) Replication() map[ID]ReplStatus { return info.json.Replication }
func (info cachedInfo) Trace() *Trace                  { return nil }
func (info cachedInfo) JSON() interface{}              { return info.json }

type inspect struct {
	*task
	fn func(api Info)
}

func Inspect(fn func(r Info)) Task {
	return inspect{task: newTask(), fn: fn}
}

func (r *Raft) Info() Info {
	var info Info
	task := Inspect(func(r Info) {
		info = cachedInfo{
			json: r.JSON().(json),
		}
	})
	r.taskCh <- task
	<-task.Done()
	return info
}

// ------------------------------------------------------------------------

type addNonvoter struct {
	*task
	node Node
}

func AddNonvoter(node Node) Task {
	return addNonvoter{task: newTask(), node: node}
}

type removeNode struct {
	*task
	id ID
}

func RemoveNode(id ID) Task {
	return removeNode{task: newTask(), id: id}
}

type changeAddrs struct {
	*task
	addrs map[ID]string
}

func ChangeAddrs(addrs map[ID]string) Task {
	return changeAddrs{task: newTask(), addrs: addrs}
}

// result is of type SnapshotMeta
type takeSnapshot struct {
	*task
	lastSnapIndex uint64
	threshold     uint64
	config        Config
}

func TakeSnapshot(threshold uint64) Task {
	return takeSnapshot{task: newTask(), threshold: threshold}
}

type fsmSnapReq struct {
	*task
	index uint64
}

type fsmSnapResp struct {
	index uint64
	term  uint64
	state FSMState
}

type fsmRestoreReq struct {
	*task
	index uint64
}

// ------------------------------------------------------------------------

func (r *Raft) executeTask(t Task) {
	if r.shutdownCalled() {
		t.reply(ErrServerClosed)
		return
	}

	switch t := t.(type) {
	case bootstrap:
		r.bootstrap(t)
	case takeSnapshot:
		t.lastSnapIndex = r.snapIndex
		t.config = r.configs.Committed
		r.snapTaskCh <- t
	case inspect:
		t.fn(liveInfo{r: r})
		t.reply(nil)
	default:
		t.reply(NotLeaderError{r.leaderAddr(), false})
	}
}

func (ldr *leadership) executeTask(t Task) {
	if ldr.shutdownCalled() {
		t.reply(ErrServerClosed)
		return
	}

	switch t := t.(type) {
	case NewEntry:
		t.reply(errors.New("raft: use Raft.NewEntries() for NewEntry"))
	case addNonvoter:
		ldr.addNonvoter(t)
	case removeNode:
		ldr.removeNode(t)
	case changeAddrs:
		ldr.changeAddrs(t)
	case inspect:
		t.fn(liveInfo{r: ldr.Raft, ldr: ldr})
		t.reply(nil)
	default:
		ldr.Raft.executeTask(t)
	}
}
