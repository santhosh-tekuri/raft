// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	newEntry() *newEntry
}

type newEntry struct {
	cmd interface{}
	*task
	*entry
	next *newEntry
}

func (ne *newEntry) newEntry() *newEntry {
	return ne
}

func (r *Raft) FSMTasks() chan<- FSMTask {
	return r.fsmTaskCh
}

func (r *Raft) runBatch() {
	var neHead, neTail *newEntry
	newEntryCh := r.newEntryCh
	i := 0
	for {
		select {
		case <-r.close:
			if neHead != nil {
				r.newEntryCh <- neHead
			}
			close(r.newEntryCh)
			return
		case t := <-r.fsmTaskCh:
			i++
			ne := t.newEntry()
			if neTail != nil {
				neTail.next, neTail = ne, ne
			} else {
				neHead, neTail = ne, ne
				newEntryCh = r.newEntryCh
			}
		case newEntryCh <- neHead:
			if trace {
				println(r, "got batch of", i, "entries")
			}
			i = 0
			neHead, neTail = nil, nil
			newEntryCh = nil
		}
	}
}

func fsmTask(typ entryType, cmd interface{}, data []byte) FSMTask {
	return &newEntry{
		task:  newTask(),
		cmd:   cmd,
		entry: &entry{typ: typ, data: data},
	}
}

func UpdateFSM(data []byte) FSMTask {
	return fsmTask(entryUpdate, nil, data)
}

func ReadFSM(cmd interface{}) FSMTask {
	return fsmTask(entryRead, cmd, nil)
}

// BarrierFSM is used to issue a command that blocks until all preceding
// commands have been applied to the FSM. It can be used to ensure the
// FSM reflects all queued commands.
func BarrierFSM() FSMTask {
	return fsmTask(entryBarrier, nil, nil)
}

// ------------------------------------------------------------------------

type infoTask struct {
	*task
}

func GetInfo() Task {
	return infoTask{newTask()}
}

func (r *Raft) info() Info {
	var flrs map[uint64]FlrStatus
	if r.state == Leader {
		flrs := make(map[uint64]FlrStatus)
		for id, repl := range r.ldr.repls {
			errMessage := ""
			if repl.status.err != nil {
				errMessage = repl.status.err.Error()
			}
			var round uint64
			if repl.status.round != nil {
				round = repl.status.round.Ordinal
			}
			var unreachable *time.Time
			if !repl.status.noContact.IsZero() {
				unreachable = &repl.status.noContact
			}
			flrs[id] = FlrStatus{
				ID:          id,
				MatchIndex:  repl.status.matchIndex,
				Unreachable: unreachable,
				Err:         repl.status.err,
				ErrMessage:  errMessage,
				Round:       round,
			}
		}
	}
	return Info{
		CID:           r.cid,
		NID:           r.nid,
		Addr:          r.addr(),
		Term:          r.term,
		State:         r.state,
		Leader:        r.leader,
		SnapshotIndex: r.snaps.index,
		FirstLogIndex: r.log.PrevIndex() + 1,
		LastLogIndex:  r.lastLogIndex,
		LastLogTerm:   r.lastLogTerm,
		Committed:     r.commitIndex,
		LastApplied:   r.lastApplied(),
		Configs:       r.configs.clone(),
		Followers:     flrs,
	}
}

type FlrStatus struct {
	ID          uint64     `json:"-"`
	MatchIndex  uint64     `json:"matchIndex"`
	Unreachable *time.Time `json:"unreachable,omitempty"`
	Err         error      `json:"-"`
	ErrMessage  string     `json:"error,omitempty"`
	Round       uint64     `json:"round,omitempty"`
}

type Info struct {
	CID           uint64               `json:"cid"`
	NID           uint64               `json:"nid"`
	Addr          string               `json:"addr"`
	Term          uint64               `json:"term"`
	State         State                `json:"state"`
	Leader        uint64               `json:"leader,omitempty"`
	SnapshotIndex uint64               `json:"snapshotIndex"`
	FirstLogIndex uint64               `json:"firstLogIndex"`
	LastLogIndex  uint64               `json:"lastLogIndex"`
	LastLogTerm   uint64               `json:"lastLogTerm"`
	Committed     uint64               `json:"committed"`
	LastApplied   uint64               `json:"lastApplied"`
	Configs       Configs              `json:"configs"`
	Followers     map[uint64]FlrStatus `json:"followers,omitempty"`
}

// ------------------------------------------------------------------------

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

func (c *Config) SetData(id uint64, data string) error {
	n, ok := c.Nodes[id]
	if !ok {
		return fmt.Errorf("raft: node %d not found", id)
	}
	n.Data = data
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
	case infoTask:
		t.reply(r.info())
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
			t.reply(notLeaderError(r, false))
		}
	}
}

func (l *leader) executeTask(t Task) {
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
