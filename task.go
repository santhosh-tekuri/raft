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
	r *Raft
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
	if info.r.state != Leader {
		return nil
	}
	m := make(map[ID]ReplStatus)
	for id, repl := range info.r.ldr.repls {
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
	select {
	case <-r.shutdownCh:
		return nil
	case r.taskCh <- task:
		<-task.Done()
		return info
	}
}

// ------------------------------------------------------------------------

type ChangeConfig struct {
	*task
	add    map[ID]bool // [id]promote
	remove map[ID]struct{}
	addrs  map[ID]string
}

func (c *ChangeConfig) AddNonVoter(id ID, addr string, promote bool) {
	c.add[id] = promote
	c.addrs[id] = addr
}

func (c *ChangeConfig) Remove(ids ...ID) {
	for _, id := range ids {
		c.remove[id] = struct{}{}
	}
}

func (c *ChangeConfig) ChangeAddr(id ID, addr string) {
	c.addrs[id] = addr
}

func NewChangeConfig() ChangeConfig {
	return ChangeConfig{
		task:   newTask(),
		add:    make(map[ID]bool),
		remove: make(map[ID]struct{}),
		addrs:  make(map[ID]string),
	}
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

// snapLoop sends this to raft, after snapshot taken
type snapTaken struct {
	req  takeSnapshot
	meta SnapshotMeta
	err  error
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
}

// ------------------------------------------------------------------------

func (r *Raft) executeTask(t Task) {
	switch t := t.(type) {
	case bootstrap:
		r.bootstrap(t)
	case takeSnapshot:
		if r.snapTakenCh != nil {
			t.reply("in progress")
			return
		}
		t.lastSnapIndex = r.snapIndex
		t.config = r.configs.Committed
		r.snapTakenCh = make(chan snapTaken)
		go r.takeSnapshot(t)
	case inspect:
		t.fn(r.liveInfo())
		t.reply(nil)
	default:
		if r.state == Leader {
			r.ldr.executeTask(t)
		} else {
			t.reply(NotLeaderError{r.leaderAddr(), false})
		}
	}
}

var errInvalidTask = errors.New("raft: invalid task")

func (l *ldrShip) executeTask(t Task) {
	switch t := t.(type) {
	case NewEntry:
		t.reply(errors.New("raft: use Raft.NewEntries() for NewEntry"))
	case ChangeConfig:
		l.changeConfig(t)
	default:
		t.reply(errInvalidTask)
	}
}

func (r *Raft) onSnapshotTaken(t snapTaken) {
	r.snapTakenCh = nil

	if t.err != nil {
		t.req.reply(t.err)
		return
	}
	// todo: we can check repl status and accordingly decide how much to delete
	metaIndexExists := t.meta.Index > r.snapIndex && t.meta.Index <= r.lastLogIndex
	if metaIndexExists {
		if err := r.storage.deleteLTE(t.meta); err != nil {
			// send to trace
			assert(false, err.Error())
		}
	}
	t.req.reply(t.meta)
}
