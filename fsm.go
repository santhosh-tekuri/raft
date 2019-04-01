package raft

import (
	"bufio"
	"io"
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
	id uint64

	taskCh chan Task
	snaps  *snapshots
}

func (fsm *stateMachine) runLoop() {
	var lastAppliedIndex, lastAppliedTerm uint64
	for t := range fsm.taskCh {
		switch t := t.(type) {
		case newEntry:
			debug(fsm.id, "fsm.execute", t.typ, t.index)
			var resp interface{}
			if t.typ == entryUpdate {
				resp = fsm.Update(t.entry.data)
				lastAppliedIndex, lastAppliedTerm = t.index, t.term
			} else if t.typ == entryRead {
				resp = fsm.Read(t.entry.data)
			}
			t.reply(resp)
		case fsmSnapReq:
			if lastAppliedIndex == 0 {
				t.reply(ErrNoUpdates)
				continue
			}
			if lastAppliedIndex < t.index {
				t.reply(ErrSnapshotThreshold)
				continue
			}
			state, err := fsm.Snapshot()
			if err != nil {
				debug(fsm, "fsm.Snapshot failed", err)
				// send to trace
				t.reply(err)
				continue
			}
			t.reply(fsmSnapResp{
				index: lastAppliedIndex,
				term:  lastAppliedTerm,
				state: state,
			})
		case fsmRestoreReq:
			meta, sr, err := fsm.snaps.open()
			if err != nil {
				debug(fsm, "snapshots.open failed", err)
				t.reply(opError(err, "snapshots.open"))
				continue
			}
			if err = fsm.RestoreFrom(sr); err != nil {
				debug(fsm, "fsm.restore failed", err)
				// todo: detect where err occurred in restoreFrom/sr.read
				t.reply(opError(err, "FSM.RestoreFrom"))
			} else {
				lastAppliedIndex, lastAppliedTerm = meta.Index, meta.Term
				debug(fsm, "restored snapshot", meta.Index)
				t.reply(nil)
			}
			_ = sr.Close()
		}
	}
}

func (r *Raft) applyEntry(ne newEntry) {
	switch ne.typ {
	case entryNop:
		// do nothing
	case entryConfig:
		// we already processed in setCommitIndex
		ne.reply(nil)
	case entryUpdate:
		debug(r, "fms <- {", ne.typ, ne.index, "}")
		select {
		case <-r.close:
			ne.reply(ErrServerClosed)
		case r.fsm.taskCh <- ne:
		}
	default:
		fatal("raft.applyEntry: type %d", ne.typ)
	}
}

// --------------------------------------------------------------------------

// todo: trace snapshot start and finish
func (r *Raft) onTakeSnapshot(t takeSnapshot) {
	if r.snapTakenCh != nil {
		t.reply(InProgressError("takeSnapshot"))
		return
	}
	r.snapTakenCh = make(chan snapTaken, 1)
	go func(index uint64, config Config) { // tracked by r.snapTakenCh
		meta, err := doTakeSnapshot(r.fsm, index, config)
		r.snapTakenCh <- snapTaken{
			req:  t,
			meta: meta,
			err:  err,
		}
	}(r.snaps.index+t.threshold, r.configs.Committed)
}

func doTakeSnapshot(fsm *stateMachine, index uint64, config Config) (meta SnapshotMeta, err error) {
	// get fsm state
	req := fsmSnapReq{task: newTask(), index: index}
	fsm.taskCh <- req
	<-req.Done()
	if req.Err() != nil {
		err = req.Err()
		return
	}
	resp := req.Result().(fsmSnapResp)
	defer resp.state.Release()

	// write snapshot to storage
	debug(fsm, "takingSnap:", resp.index)
	sink, err := fsm.snaps.new(resp.index, resp.term, config)
	if err != nil {
		debug(fsm, "snapshots.new failed", err)
		err = opError(err, "snapshots.new")
		return
	}
	bufw := bufio.NewWriter(sink.file)
	err = resp.state.WriteTo(bufw)
	if err == nil {
		err = bufw.Flush()
	}
	meta, doneErr := sink.done(err)
	if err != nil {
		debug(fsm, "FSMState.WriteTo failed", resp.index, err)
		err = opError(err, "FSMState.WriteTo")
		return
	}
	if doneErr != nil {
		debug(fsm, "snapshotSink.done failed", resp.index, err)
		err = opError(err, "snapshotSink.done")
	}
	return
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

	if r.storage.log.Contains(t.meta.Index) {
		// find compact index
		// nowCompact: min of all matchIndex
		// canCompact: min of online matchIndex
		nowCompact, canCompact := t.meta.Index, t.meta.Index
		if r.state == Leader {
			for _, f := range r.ldr.flrs {
				if f.status.matchIndex < nowCompact {
					nowCompact = f.status.matchIndex
				}
				if f.status.noContact.IsZero() && f.status.matchIndex < canCompact {
					canCompact = f.status.matchIndex
				}
			}
		}
		debug(r, "nowCompact:", nowCompact, "canCompact:", canCompact)
		nowCompact, canCompact = r.log.CanLTE(nowCompact), r.log.CanLTE(canCompact)
		debug(r, "nowCompact:", nowCompact, "canCompact:", canCompact)
		if nowCompact > r.log.PrevIndex() {
			r.compactLog(nowCompact)
		}
		if canCompact > nowCompact {
			// notify flrs with new logView
			r.ldr.removeLTE = canCompact
			r.ldr.notifyFlr(false)
		}
	}
	t.req.reply(t.meta.Index)
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
	meta SnapshotMeta
	err  error
}

// ---------------------------------------------------------------------------

func (r *Raft) restoreFSM() error {
	req := fsmRestoreReq{task: newTask()}
	r.fsm.taskCh <- req
	<-req.Done()
	if req.Err() != nil {
		return req.Err()
	}
	r.commitIndex, r.lastApplied = r.snaps.index, r.snaps.index
	return nil
}

// raft(onRestart/onInstallSnapReq) -> fsmLoop
type fsmRestoreReq struct {
	*task
}
