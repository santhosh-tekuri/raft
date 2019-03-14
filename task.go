package raft

import (
	"errors"
	"fmt"
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

func UpdateFSM(data []byte) NewEntry {
	return newEntry(entryUpdate, data)
}

func ReadFSM(data []byte) NewEntry {
	return newEntry(entryRead, data)
}

// BarrierFSM is used to issue a command that blocks until all preceding
// commands have been applied to the FSM. It can be used to ensure the
// FSM reflects all queued commands.
func BarrierFSM() NewEntry {
	return newEntry(entryBarrier, nil)
}

// ------------------------------------------------------------------------

type bootstrap struct {
	*task
	nodes map[uint64]Node
}

func Bootstrap(nodes map[uint64]Node) Task {
	return bootstrap{task: newTask(), nodes: nodes}
}

// ------------------------------------------------------------------------

type FlrStatus struct {
	ID          uint64    `json:"-"`
	MatchIndex  uint64    `json:"matchIndexes"`
	Unreachable time.Time `json:"unreachable,omitempty"`
	Rounds      uint64    `json:"rounds,omitempty"`
}

type json struct {
	ID           uint64               `json:"id"`
	Addr         string               `json:"addr"`
	Term         uint64               `json:"term"`
	State        State                `json:"state"`
	Leader       uint64               `json:"leader,omitempty"`
	LastLogIndex uint64               `json:"lastLogIndex"`
	LastLogTerm  uint64               `json:"lastLogTerm"`
	Committed    uint64               `json:"committed"`
	LastApplied  uint64               `json:"lastApplied"`
	Configs      Configs              `json:"configs"`
	Followers    map[uint64]FlrStatus `json:"followers,omitempty"`
}

type Info interface {
	ID() uint64
	Addr() string
	Term() uint64
	State() State
	Leader() uint64
	LastLogIndex() uint64
	LastLogTerm() uint64
	Committed() uint64
	LastApplied() uint64
	Configs() Configs
	Followers() map[uint64]FlrStatus
	JSON() interface{}
}

type liveInfo struct {
	r *Raft
}

func (info liveInfo) ID() uint64           { return info.r.id }
func (info liveInfo) Addr() string         { return info.r.addr() }
func (info liveInfo) Term() uint64         { return info.r.term }
func (info liveInfo) State() State         { return info.r.state }
func (info liveInfo) Leader() uint64       { return info.r.leader }
func (info liveInfo) LastLogIndex() uint64 { return info.r.lastLogIndex }
func (info liveInfo) LastLogTerm() uint64  { return info.r.lastLogTerm }
func (info liveInfo) Committed() uint64    { return info.r.commitIndex }
func (info liveInfo) LastApplied() uint64  { return info.r.lastApplied }
func (info liveInfo) Configs() Configs     { return info.r.configs.clone() }

func (info liveInfo) Followers() map[uint64]FlrStatus {
	if info.r.state != Leader {
		return nil
	}
	flrs := make(map[uint64]FlrStatus)
	for id, f := range info.r.ldr.flrs {
		flrs[id] = FlrStatus{
			MatchIndex:  f.status.matchIndex,
			Unreachable: f.status.noContact,
			Rounds:      f.status.rounds,
		}
	}
	return flrs
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
		Followers:    info.Followers(),
	}
}

type cachedInfo struct {
	json json
}

func (info cachedInfo) ID() uint64                      { return info.json.ID }
func (info cachedInfo) Addr() string                    { return info.json.Addr }
func (info cachedInfo) Term() uint64                    { return info.json.Term }
func (info cachedInfo) State() State                    { return info.json.State }
func (info cachedInfo) Leader() uint64                  { return info.json.Leader }
func (info cachedInfo) LastLogIndex() uint64            { return info.json.LastLogIndex }
func (info cachedInfo) LastLogTerm() uint64             { return info.json.LastLogTerm }
func (info cachedInfo) Committed() uint64               { return info.json.Committed }
func (info cachedInfo) LastApplied() uint64             { return info.json.LastApplied }
func (info cachedInfo) Configs() Configs                { return info.json.Configs }
func (info cachedInfo) Followers() map[uint64]FlrStatus { return info.json.Followers }
func (info cachedInfo) JSON() interface{}               { return info.json }

type inspect struct {
	*task
	fn func(r *Raft)
}

func (r *Raft) inspect(fn func(r *Raft)) error {
	t := inspect{task: newTask(), fn: fn}
	select {
	case <-r.shutdownCh:
		return ErrServerClosed
	case r.taskCh <- t:
		<-t.Done()
		return nil
	}
}
func (r *Raft) Info() Info {
	var info Info
	_ = r.inspect(func(r *Raft) {
		info = cachedInfo{
			json: r.liveInfo().JSON().(json),
		}
	})
	return info
}

func (r *Raft) SetTrace(trace Trace) error {
	return r.inspect(func(*Raft) {
		r.trace = trace
	})
}

// ------------------------------------------------------------------------

func (c *Config) AddNonVoter(id uint64, addr string, promote bool) error {
	if id == 0 {
		return errors.New("raft: id must be greater than zero")
	}
	if _, ok := c.Nodes[id]; ok {
		return fmt.Errorf("raft: node %d already exists", id)
	}
	n := Node{ID: id, Addr: addr, Promote: promote}
	if err := n.validate(); err != nil {
		return err
	}
	c.Nodes[id] = n
	return nil
}

func (c *Config) Remove(id uint64) error {
	if _, ok := c.Nodes[id]; !ok {
		return fmt.Errorf("raft: node %d not found", id)
	}
	delete(c.Nodes, id)
	return nil
}

func (c *Config) ChangeAddr(id uint64, addr string) error {
	n, ok := c.Nodes[id]
	if !ok {
		return fmt.Errorf("raft: node %d not found", id)
	}
	n.Addr = addr
	if err := n.validate(); err != nil {
		return err
	}
	cn, ok := c.nodeForAddr(addr)
	if ok && cn.ID != id {
		return fmt.Errorf("raft: address %s is used by node %d", addr, cn.ID)
	}
	c.Nodes[id] = n
	return nil
}

func (c *Config) Promote(id uint64) error {
	n, ok := c.Nodes[id]
	if !ok {
		return fmt.Errorf("raft: node %d not found", id)
	}
	if n.Voter {
		return errors.New("raft: only nonvoters can be promoted")
	}
	n.Promote = true
	c.Nodes[id] = n
	return nil
}

func (c *Config) Demote(id uint64) error {
	n, ok := c.Nodes[id]
	if !ok {
		return fmt.Errorf("raft: node %d not found", id)
	}
	n.Voter = false
	n.Promote = false
	c.Nodes[id] = n
	return nil
}

type changeConfig struct {
	*task
	newConf Config
}

func ChangeConfig(newConf Config) Task {
	return changeConfig{
		task:    newTask(),
		newConf: newConf.clone(),
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

type transferLdr struct {
	*task
	timeout time.Duration
	term    uint64
	rpcCh   <-chan error // non-nil, when transfer in progress and timeoutNow request sent
}

func TransferLeadership(timeout time.Duration) Task {
	return transferLdr{
		task:    newTask(),
		timeout: timeout,
	}
}

// ------------------------------------------------------------------------

func (r *Raft) executeTask(t Task) {
	switch t := t.(type) {
	case bootstrap:
		r.bootstrap(t)
	case takeSnapshot:
		if r.snapTakenCh != nil {
			t.reply(InProgressError("takeSnapshot"))
			return
		}
		t.lastSnapIndex = r.snapIndex
		t.config = r.configs.Committed
		r.snapTakenCh = make(chan snapTaken)
		go r.takeSnapshot(t) // goroutine tracked by r.snapTakenCh
	case inspect:
		t.fn(r)
		t.reply(nil)
	default:
		if r.state == Leader {
			r.ldr.executeTask(t)
		} else {
			t.reply(NotLeaderError{r.leaderAddr(), false, nil})
		}
	}
}

func (l *ldrShip) executeTask(t Task) {
	switch t := t.(type) {
	case NewEntry:
		t.reply(errors.New("raft: use Raft.NewEntries() for NewEntry"))
	case changeConfig:
		l.changeConfig(t)
	case transferLdr:
		l.onTransferLeadership(t)
	default:
		t.reply(errInvalidTask)
	}
}
