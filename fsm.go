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
	"bufio"
	"bytes"
	"io"

	"github.com/santhosh-tekuri/raft/log"
)

type FSM interface {
	Update(cmd []byte) interface{}
	Read(cmd []byte) interface{}
	Snapshot() (FSMState, error)
	RestoreFrom(io.Reader) error
}

type FSMState interface {
	WriteTo(w io.Writer) error
	Release()
}

type stateMachine struct {
	FSM
	id    uint64
	index uint64
	term  uint64
	ch    chan interface{}
	snaps *snapshots
}

func (fsm *stateMachine) runLoop() {
	// todo: panics are not handled by Raft
	for t := range fsm.ch {
		if trace {
			debug(fsm, t)
		}
		switch t := t.(type) {
		case fsmApply:
			fsm.onApply(t)
		case fsmSnapReq:
			fsm.onSnapReq(t)
		case fsmRestoreReq:
			err := fsm.onRestoreReq()
			if trace {
				if err != nil {
					debug(fsm, "fsmRestore failed", err)
				} else {
					debug(fsm, "restored snapshot", fsm.index)
				}
			}
			t.err <- err
		case lastApplied:
			t.reply(fsm.index)
		}
	}
}

func (fsm *stateMachine) onApply(t fsmApply) {
	// process all entries before t.neHead from log
	commitIndex := t.log.LastIndex()
	front := commitIndex + 1
	if t.neHead != nil {
		front = t.neHead.index
	}
	for fsm.index+1 < front {
		b, err := t.log.Get(fsm.index + 1)
		if err != nil {
			panic(opError(err, "Log.Get(%d)", fsm.index+1))
		}
		e := &entry{}
		if err := e.decode(bytes.NewReader(b)); err != nil {
			panic(opError(err, "Log.Get(%d).decode", fsm.index+1))
		}
		if e.index != fsm.index+1 {
			panic(bug(1, "e.index=%d, fsm.index=%d", e.index, fsm.index))
		}
		if trace {
			debug(fsm, "apply", e.typ, e.index)
		}
		if e.typ == entryUpdate {
			fsm.Update(e.data)
		}
		fsm.index, fsm.term = e.index, e.term
	}

	// process all entries from t.neHead if any
	for ne := t.neHead; ne != nil; ne = ne.next {
		if ne.index != fsm.index+1 {
			panic(bug(1, "ne.index=%d, fsm.index=%d", ne.index, fsm.index))
		}
		if trace {
			debug(fsm, "apply", ne.typ, ne.index)
		}
		var resp interface{}
		if ne.typ == entryRead {
			resp = fsm.Read(ne.data)
		} else if ne.typ == entryUpdate {
			resp = fsm.Update(ne.data)
		}
		if ne.isLogEntry() {
			fsm.index, fsm.term = ne.index, ne.term
		}
		ne.reply(resp)
	}

	if fsm.index != commitIndex {
		panic(bug(1, "fsm.index=%d, commitIndex=%d", fsm.index, commitIndex))
	}
}

func (fsm *stateMachine) onSnapReq(t fsmSnapReq) {
	if fsm.index == 0 {
		t.reply(ErrNoUpdates)
		return
	}
	if fsm.index < t.index {
		t.reply(ErrSnapshotThreshold)
		return
	}
	state, err := fsm.Snapshot()
	if err != nil {
		if trace {
			debug(fsm, "fsm.Snapshot failed", err)
		}
		t.reply(opError(err, "fsm.Snapshot"))
		return
	}
	t.reply(fsmSnapResp{
		index: fsm.index,
		term:  fsm.term,
		state: state,
	})
}

func (fsm *stateMachine) onRestoreReq() error {
	snap, err := fsm.snaps.open()
	if err != nil {
		return opError(err, "snapshots.open")
	}
	defer snap.release()
	if err = fsm.RestoreFrom(bufio.NewReader(snap.file)); err != nil {
		return opError(err, "FSM.RestoreFrom")
	}
	fsm.index, fsm.term = snap.meta.index, snap.meta.term
	return nil
}

type fsmApply struct {
	neHead *newEntry
	log    *log.Log
}

type lastApplied struct {
	*task
}

func (r *Raft) lastApplied() uint64 {
	t := lastApplied{newTask()}
	r.fsm.ch <- t
	<-t.done
	return t.result.(uint64)
}

// raft(onRestart/onInstallSnapReq) -> fsmLoop
type fsmRestoreReq struct {
	err chan error
}

// takeSnapshot --------------------------------------------------------------------------

// todo: trace snapshot start and finish
func (r *Raft) onTakeSnapshot(t takeSnapshot) {
	if r.snapTakenCh != nil {
		t.reply(InProgressError("takeSnapshot"))
		return
	}
	r.snapTakenCh = make(chan snapTaken, 1)
	go func(index uint64, config Config) { // tracked by r.snapTakenCh
		meta, err := doTakeSnapshot(r.fsm, index, config)
		if trace {
			debug(r, "doTakeSnapshot err:", err)
		}
		r.snapTakenCh <- snapTaken{
			req:  t,
			meta: meta,
			err:  err,
		}
	}(r.snaps.index+t.threshold, r.configs.Committed)
}

func doTakeSnapshot(fsm *stateMachine, index uint64, config Config) (snapshotMeta, error) {
	// get fsm state
	req := fsmSnapReq{task: newTask(), index: index}
	fsm.ch <- req
	<-req.Done()
	if req.Err() != nil {
		return snapshotMeta{}, req.Err()
	}
	resp := req.Result().(fsmSnapResp)
	defer resp.state.Release()

	// write snapshot to storage
	sink, err := fsm.snaps.new(resp.index, resp.term, config)
	if err != nil {
		return snapshotMeta{}, opError(err, "snapshots.new")
	}
	bufw := bufio.NewWriter(sink.file)
	err = resp.state.WriteTo(bufw)
	if err == nil {
		err = bufw.Flush()
	}
	meta, doneErr := sink.done(err)
	if err != nil {
		return meta, opError(err, "FSMState.WriteTo")
	}
	if doneErr != nil {
		return meta, opError(err, "snapshotSink.done")
	}
	return meta, nil
}

func (r *Raft) onSnapshotTaken(t snapTaken) {
	r.snapTakenCh = nil // clear in progress flag

	if t.err != nil {
		if err, ok := t.err.(OpError); ok && r.trace.Error != nil {
			r.trace.Error(err)
		}
		t.req.reply(t.err)
		return
	}

	if r.storage.log.Contains(t.meta.index) {
		// find compact index
		// nowCompact: min of all matchIndex
		// canCompact: min of online matchIndex
		nowCompact, canCompact := t.meta.index, t.meta.index
		if r.state == Leader {
			for _, repl := range r.ldr.repls {
				if repl.status.matchIndex < nowCompact {
					nowCompact = repl.status.matchIndex
				}
				if repl.status.noContact.IsZero() && repl.status.matchIndex < canCompact {
					canCompact = repl.status.matchIndex
				}
			}
		}
		if trace {
			debug(r, "nowCompact:", nowCompact, "canCompact:", canCompact)
		}
		nowCompact, canCompact = r.log.CanLTE(nowCompact), r.log.CanLTE(canCompact)
		if trace {
			debug(r, "nowCompact:", nowCompact, "canCompact:", canCompact)
		}
		if nowCompact > r.log.PrevIndex() {
			_ = r.compactLog(nowCompact)
		}
		if canCompact > nowCompact {
			// notify repls with new logView
			r.ldr.removeLTE = canCompact
			r.ldr.notifyFlr(false)
		}
	}
	t.req.reply(t.meta.index)
}

// takeSnapshot() -> fsmLoop
type fsmSnapReq struct {
	*task
	index uint64
}

// takeSnapshot() <- fsmLoop
type fsmSnapResp struct {
	index uint64
	term  uint64
	state FSMState
}

// snapLoop -> raft (after snapshot taken)
type snapTaken struct {
	req  takeSnapshot
	meta snapshotMeta
	err  error
}
