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
	"io"
	"time"
)

// Tasks returns a channel to which non-FSMTasks
// has to be submitted. Should be used as below:
// 	 select {
//       case <-r.Closed():
//       case r.Tasks() <- t:
//   }
func (r *Raft) Tasks() chan<- Task {
	return r.taskCh
}

// Task represents a raft task.
type Task interface {
	// Done returns a channel that is closed when task is completed.
	Done() <-chan struct{}

	// Err returns the error if any. Must be called only one completed task.
	Err() error

	// Result returns the result. Must be called only one completed task.
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

// FSMTask represents FSM related task.
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

// FSMTasks returns a channel to which FSMTasks
// has to be submitted. Should be used as below:
// 	 select {
//       case <-r.Closed():
//       case r.FSMTasks() <- t:
//   }
func (r *Raft) FSMTasks() chan<- FSMTask {
	return r.fsmTaskCh
}

func (r *Raft) runBatch() {
	var neHead, neTail *newEntry
	var newEntryCh chan *newEntry
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

// UpdateFSM task is used to modify state in FSM.
// This eventually calls FSM.Update(data).
func UpdateFSM(data []byte) FSMTask {
	return fsmTask(entryUpdate, nil, data)
}

// ReadFSM task is used to read state from FSM.
// This eventually calls FSM.Read(cmd).
func ReadFSM(cmd interface{}) FSMTask {
	return fsmTask(entryRead, cmd, nil)
}

// DirtyReadFSM task is used to read state from FSM.
// This eventually calls FSM.Read(cmd). Unlike ReadFSM
// task, this task can be submitted to non-voter.
func DirtyReadFSM(cmd interface{}) FSMTask {
	return fsmTask(entryDirtyRead, cmd, nil)
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

// GetInfo task is used to get raft state information.
// This rask returns Info object.
func GetInfo() Task {
	return infoTask{newTask()}
}

func (r *Raft) info() Info {
	var flrs map[uint64]Replication
	if r.state == Leader {
		flrs = make(map[uint64]Replication)
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
			flrs[id] = Replication{
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

// Replication captures state of replication of a node.
type Replication struct {
	ID          uint64     `json:"-"`
	MatchIndex  uint64     `json:"matchIndex"`
	Unreachable *time.Time `json:"unreachable,omitempty"`
	Err         error      `json:"-"`
	ErrMessage  string     `json:"error,omitempty"`
	Round       uint64     `json:"round,omitempty"`
}

func (repl *Replication) decode(r io.Reader) error {
	var err error
	if repl.ID, err = readUint64(r); err != nil {
		return err
	}
	if repl.MatchIndex, err = readUint64(r); err != nil {
		return err
	}
	unixNano, err := readUint64(r)
	if err != nil {
		return err
	}
	if unixNano != 0 {
		t := time.Unix(0, int64(unixNano))
		repl.Unreachable = &t
	}
	if repl.ErrMessage, err = readString(r); err != nil {
		return err
	}
	if repl.ErrMessage != "" {
		repl.Err = errors.New(repl.ErrMessage)
	}
	repl.Round, err = readUint64(r)
	return err
}

func (repl *Replication) encode(w io.Writer) error {
	if err := writeUint64(w, repl.ID); err != nil {
		return err
	}
	if err := writeUint64(w, repl.MatchIndex); err != nil {
		return err
	}
	var unixNano uint64
	if repl.Unreachable != nil {
		unixNano = uint64(repl.Unreachable.UnixNano())
	}
	if err := writeUint64(w, unixNano); err != nil {
		return err
	}
	if err := writeString(w, repl.ErrMessage); err != nil {
		return err
	}
	return writeUint64(w, repl.Round)
}

// Info captures state of a node.
type Info struct {
	CID           uint64                 `json:"cid"`
	NID           uint64                 `json:"nid"`
	Addr          string                 `json:"addr"`
	Term          uint64                 `json:"term"`
	State         State                  `json:"state"`
	Leader        uint64                 `json:"leader,omitempty"`
	SnapshotIndex uint64                 `json:"snapshotIndex"`
	FirstLogIndex uint64                 `json:"firstLogIndex"`
	LastLogIndex  uint64                 `json:"lastLogIndex"`
	LastLogTerm   uint64                 `json:"lastLogTerm"`
	Committed     uint64                 `json:"committed"`
	LastApplied   uint64                 `json:"lastApplied"`
	Configs       Configs                `json:"configs"`
	Followers     map[uint64]Replication `json:"followers,omitempty"`
}

func (info *Info) decode(r io.Reader) error {
	var err error
	if info.CID, err = readUint64(r); err != nil {
		return err
	}
	if info.NID, err = readUint64(r); err != nil {
		return err
	}
	if info.Addr, err = readString(r); err != nil {
		return err
	}
	if info.Term, err = readUint64(r); err != nil {
		return err
	}
	b, err := readUint8(r)
	if err != nil {
		return err
	}
	info.State = State(b)
	if info.Leader, err = readUint64(r); err != nil {
		return err
	}
	if info.SnapshotIndex, err = readUint64(r); err != nil {
		return err
	}
	if info.FirstLogIndex, err = readUint64(r); err != nil {
		return err
	}
	if info.LastLogIndex, err = readUint64(r); err != nil {
		return err
	}
	if info.LastLogTerm, err = readUint64(r); err != nil {
		return err
	}
	if info.Committed, err = readUint64(r); err != nil {
		return err
	}
	if info.LastApplied, err = readUint64(r); err != nil {
		return err
	}
	e := &entry{}
	if err = e.decode(r); err != nil {
		return err
	}
	if err = info.Configs.Committed.decode(e); err != nil {
		return err
	}
	if err = e.decode(r); err != nil {
		return err
	}
	if err = info.Configs.Latest.decode(e); err != nil {
		return err
	}
	sz, err := readUint32(r)
	if err != nil {
		return err
	}
	if sz > 0 {
		info.Followers = map[uint64]Replication{}
		for sz > 0 {
			sz--
			repl := Replication{}
			if err = repl.decode(r); err != nil {
				return err
			}
			info.Followers[repl.ID] = repl
		}
	}
	return nil
}

func (info Info) encode(w io.Writer) error {
	if err := writeUint64(w, info.CID); err != nil {
		return err
	}
	if err := writeUint64(w, info.NID); err != nil {
		return err
	}
	if err := writeString(w, info.Addr); err != nil {
		return err
	}
	if err := writeUint64(w, info.Term); err != nil {
		return err
	}
	if err := writeUint8(w, uint8(info.State)); err != nil {
		return err
	}
	if err := writeUint64(w, info.Leader); err != nil {
		return err
	}
	if err := writeUint64(w, info.SnapshotIndex); err != nil {
		return err
	}
	if err := writeUint64(w, info.FirstLogIndex); err != nil {
		return err
	}
	if err := writeUint64(w, info.LastLogIndex); err != nil {
		return err
	}
	if err := writeUint64(w, info.LastLogTerm); err != nil {
		return err
	}
	if err := writeUint64(w, info.Committed); err != nil {
		return err
	}
	if err := writeUint64(w, info.LastApplied); err != nil {
		return err
	}
	if err := info.Configs.Committed.encode().encode(w); err != nil {
		return err
	}
	if err := info.Configs.Latest.encode().encode(w); err != nil {
		return err
	}
	flrs := info.Followers
	if err := writeUint32(w, uint32(len(flrs))); err != nil {
		return err
	}
	for _, flr := range flrs {
		if err := flr.encode(w); err != nil {
			return err
		}
	}
	return nil
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

type changeConfig struct {
	*task
	newConf Config
}

// ChangeConfig task applies changes to cluster provides by the actions
// specified in the newConfig. Multiple actions can be specified in
// newConfig, but leader applies them sequentially as per raft protocol.
//
// ErrStaleConfig: if newConfig.index != latestConfig.index.
// InProgressError: if there is already another TakeSnapshot task is in progress.
//                  or if latest config is not committed i.e, another configChange step is in progress.
func ChangeConfig(newConf Config) Task {
	return changeConfig{
		task:    newTask(),
		newConf: newConf.clone(),
	}
}

type waitForStableConfig struct {
	*task
}

// WaitForStableConfig task waits until all pending config changes are completed.
// This task returns just error if any.
func WaitForStableConfig() Task {
	return waitForStableConfig{task: newTask()}
}

// ------------------------------------------------------------------------

// result is snapshot index
type takeSnapshot struct {
	*task
	threshold uint64
}

// TakeSnapshot task takes snapshot if there are atleast threshold edits since
// last snapshot. if threshold is zero, than snapshot is taken if there is atleast
// one edit since last snapshot. This task returns the log index where snapshot is taken.
//
// ErrSnapshotThreshold: the threshold is not satisfied.
// ErrNoUpdates: there are no edits since last snapshot.
// InProgressError: if there is already another TakeSnapshot task is in progress.
func TakeSnapshot(threshold uint64) Task {
	return takeSnapshot{task: newTask(), threshold: threshold}
}

type transferLdr struct {
	*task
	target  uint64 // whom to transfer. 0 means not specified
	timeout time.Duration
}

// TransferLeadership task trasfers current leadership to given target server.
// If target is 0, then leadership is transfered to most eligible voter node.
// This task returns just error if any.
//
// During trasfer, leader rejects any new FSMTasks with InProgressError("transferLeadership").
//
// TimeoutError: leadership failed to transfer in specified timeout.
// ErrTransferNoVoter: number of voters in cluster is one.
// ErrTransferSelf: the target node is already leader.
// ErrTransferTargetNonvoter: the target node is non-voter.
// ErrTransferInvalidTarget: the target node does not exist.
// ErrQuorumUnreachable: quorum of voters is unreachable resulting loss of leadership.
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
