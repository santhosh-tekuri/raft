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
		if t.done != nil && !isClosed(t.done) {
			close(t.done)
		}
	}
}

// ------------------------------------------------------------------------

type FSMTask interface {
	Task
	newEntry() newEntry
}

type newEntry struct {
	*task
	*entry
}

func (ne newEntry) newEntry() newEntry {
	return ne
}

func (r *Raft) FSMTasks() chan<- FSMTask {
	return r.fsmTaskCh
}

func fsmTask(typ entryType, data []byte) FSMTask {
	return newEntry{
		task:  newTask(),
		entry: &entry{typ: typ, data: data},
	}
}

func UpdateFSM(data []byte) FSMTask {
	return fsmTask(entryUpdate, data)
}

func ReadFSM(data []byte) FSMTask {
	return fsmTask(entryRead, data)
}

// BarrierFSM is used to issue a command that blocks until all preceding
// commands have been applied to the FSM. It can be used to ensure the
// FSM reflects all queued commands.
func BarrierFSM() FSMTask {
	return fsmTask(entryBarrier, nil)
}

// ------------------------------------------------------------------------

type FlrStatus struct {
	ID          uint64    `json:"-"`
	MatchIndex  uint64    `json:"matchIndex"`
	Unreachable time.Time `json:"unreachable,omitempty"`
	Err         error     `json:"-"`
	ErrMessage  string    `json:"error,omitempty"`
	Round       uint64    `json:"round,omitempty"`
}

type json struct {
	CID           uint64               `json:"cid"`
	NID           uint64               `json:"nid"`
	Addr          string               `json:"addr"`
	Term          uint64               `json:"term"`
	State         State                `json:"state"`
	Leader        uint64               `json:"leader,omitempty"`
	FirstLogIndex uint64               `json:"firstLogIndex"`
	LastLogIndex  uint64               `json:"lastLogIndex"`
	LastLogTerm   uint64               `json:"lastLogTerm"`
	Committed     uint64               `json:"committed"`
	LastApplied   uint64               `json:"lastApplied"`
	Configs       Configs              `json:"configs"`
	Followers     map[uint64]FlrStatus `json:"followers,omitempty"`
}

type Info interface {
	CID() uint64
	NID() uint64
	Addr() string
	Term() uint64
	State() State
	Leader() uint64
	FirstLogIndex() uint64
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

func (info liveInfo) CID() uint64           { return info.r.cid }
func (info liveInfo) NID() uint64           { return info.r.nid }
func (info liveInfo) Addr() string          { return info.r.addr() }
func (info liveInfo) Term() uint64          { return info.r.term }
func (info liveInfo) State() State          { return info.r.state }
func (info liveInfo) Leader() uint64        { return info.r.leader }
func (info liveInfo) FirstLogIndex() uint64 { return info.r.log.PrevIndex() + 1 }
func (info liveInfo) LastLogIndex() uint64  { return info.r.lastLogIndex }
func (info liveInfo) LastLogTerm() uint64   { return info.r.lastLogTerm }
func (info liveInfo) Committed() uint64     { return info.r.commitIndex }
func (info liveInfo) LastApplied() uint64   { return info.r.lastApplied() }
func (info liveInfo) Configs() Configs      { return info.r.configs.clone() }

func (info liveInfo) Followers() map[uint64]FlrStatus {
	if info.r.state != Leader {
		return nil
	}
	flrs := make(map[uint64]FlrStatus)
	for id, f := range info.r.ldr.flrs {
		errMessage := ""
		if f.status.err != nil {
			errMessage = f.status.err.Error()
		}
		var round uint64
		if f.status.round != nil {
			round = f.status.round.Ordinal
		}
		flrs[id] = FlrStatus{
			MatchIndex:  f.status.matchIndex,
			Unreachable: f.status.noContact,
			Err:         f.status.err,
			ErrMessage:  errMessage,
			Round:       round,
		}
	}
	return flrs
}

func (info liveInfo) JSON() interface{} {
	return json{
		CID:           info.CID(),
		NID:           info.NID(),
		Addr:          info.Addr(),
		Term:          info.Term(),
		State:         info.State(),
		Leader:        info.Leader(),
		FirstLogIndex: info.FirstLogIndex(),
		LastLogIndex:  info.LastLogIndex(),
		LastLogTerm:   info.LastLogTerm(),
		Committed:     info.Committed(),
		LastApplied:   info.LastApplied(),
		Configs:       info.Configs(),
		Followers:     info.Followers(),
	}
}

type cachedInfo struct {
	json json
}

func (info cachedInfo) CID() uint64                     { return info.json.CID }
func (info cachedInfo) NID() uint64                     { return info.json.NID }
func (info cachedInfo) Addr() string                    { return info.json.Addr }
func (info cachedInfo) Term() uint64                    { return info.json.Term }
func (info cachedInfo) State() State                    { return info.json.State }
func (info cachedInfo) Leader() uint64                  { return info.json.Leader }
func (info cachedInfo) FirstLogIndex() uint64           { return info.json.FirstLogIndex }
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
	case <-r.close:
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

// AddVoter adds given node as voter.
//
// This call fails if config is not bootstrap.
func (c *Config) AddVoter(id uint64, addr string) error {
	if !c.IsBootstrap() {
		return fmt.Errorf("raft: voter cannot be added in bootstrapped config")
	}
	return c.addNode(Node{ID: id, Addr: addr, Voter: true})
}

// AddNonvoter adds given node as nonvoter.
//
// Voters can't be directly added to cluster. They must be added as
// nonvoter with promote turned on. once the node's log catches up,
// leader promotes it to voter.
func (c *Config) AddNonvoter(id uint64, addr string, promote bool) error {
	action := None
	if promote {
		action = Promote
	}
	return c.addNode(Node{ID: id, Addr: addr, Action: action})
}

func (c *Config) addNode(n Node) error {
	if err := n.validate(); err != nil {
		return err
	}
	if _, ok := c.Nodes[n.ID]; ok {
		return fmt.Errorf("raft: node %d already exists", n.ID)
	}
	c.Nodes[n.ID] = n
	return nil
}

func (c *Config) SetAction(id uint64, action ConfigAction) error {
	n, ok := c.Nodes[id]
	if !ok {
		return fmt.Errorf("raft: node %d not found", id)
	}
	n.Action = action
	if err := n.validate(); err != nil {
		return err
	}
	c.Nodes[id] = n
	return nil
}

// SetAddr changes address of given node.
//
// If you have set Options.Resolver, the address resolved
// by Resolver still takes precedence.
func (c *Config) SetAddr(id uint64, addr string) error {
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

type waitForStableConfig struct {
	*task
}

func WaitForStableConfig() Task {
	return waitForStableConfig{task: newTask()}
}

// ------------------------------------------------------------------------

// result is snapshot index
type takeSnapshot struct {
	*task
	threshold uint64
}

func TakeSnapshot(threshold uint64) Task {
	return takeSnapshot{task: newTask(), threshold: threshold}
}

type transferLdr struct {
	*task
	target  uint64 // whom to transfer. 0 means not specified
	timeout time.Duration
}

func TransferLeadership(target uint64, timeout time.Duration) Task {
	return transferLdr{
		task:    newTask(),
		target:  target,
		timeout: timeout,
	}
}

// ------------------------------------------------------------------------

// todo: reply tasks even on panic
func (r *Raft) executeTask(t Task) {
	switch t := t.(type) {
	case changeConfig:
		if r.state == Leader {
			r.ldr.executeTask(t)
		} else {
			r.bootstrap(t)
		}
	case takeSnapshot:
		r.onTakeSnapshot(t)
	case inspect:
		t.fn(r)
		t.reply(nil)
	default:
		if r.state == Leader {
			r.ldr.executeTask(t)
		} else {
			t.reply(NotLeaderError{r.leader, r.leaderAddr(), false})
		}
	}
}

func (l *ldrShip) executeTask(t Task) {
	switch t := t.(type) {
	case FSMTask:
		t.reply(errors.New("raft: use Raft.FSMTasks() for FSMTask"))
	case changeConfig:
		l.onChangeConfig(t)
	case waitForStableConfig:
		l.onWaitForStableConfig(t)
	case transferLdr:
		l.onTransfer(t)
	default:
		t.reply(errInvalidTask)
	}
}
