package raft

import (
	"io"
)

type FSM interface {
	Execute(cmd []byte) interface{}
	Snapshot() (FSMState, error)
	RestoreFrom(io.Reader) error
}

type FSMState interface {
	WriteTo(w io.Writer) error
	Release()
}

func (r *Raft) fsmLoop() {
	defer r.wg.Done()
	var updateIndex, updateTerm uint64
	for t := range r.fsmTaskCh {
		switch t := t.(type) {
		case NewEntry:
			debug(r.id, "fsm.execute", t.typ, t.index)
			var resp interface{}
			if t.typ == entryUpdate || t.typ == entryQuery {
				resp = r.fsm.Execute(t.entry.data)
				if t.typ == entryUpdate {
					updateIndex, updateTerm = t.index, t.term
				}
			}
			t.reply(resp)
		case fsmSnapReq:
			if updateIndex == 0 {
				t.reply(ErrNoUpdates)
				continue
			}
			if updateIndex < t.index {
				t.reply(ErrSnapshotThreshold)
				continue
			}
			state, err := r.fsm.Snapshot()
			if err != nil {
				debug(r, "fsm.Snapshot failed", err)
				// send to trace
				t.reply(err)
				continue
			}
			t.reply(fsmSnapResp{
				index: updateIndex,
				term:  updateTerm,
				state: state,
			})
		case fsmRestoreReq:
			meta, sr, err := r.snapshots.Open()
			if err != nil {
				debug(r, "snapshots.Open failed", err)
				t.reply(opError(err, "Snapshots.Open"))
				continue
			}
			if err = r.fsm.RestoreFrom(sr); err != nil {
				debug(r, "fsm.restore failed", err)
				// todo: detect where err occurred in restoreFrom/sr.read
				t.reply(opError(err, "FSM.RestoreFrom"))
			} else {
				updateIndex, updateTerm = meta.Index, meta.Term
				debug(r, "restored snapshot", meta.Index)
				t.reply(nil)
			}
			_ = sr.Close()
		}
	}
	debug(r.id, "fsmLoop shutdown")
}

func (r *Raft) applyEntry(ne NewEntry) {
	switch ne.typ {
	case entryNop:
		// do nothing
	case entryConfig:
		// we already processed in setCommitIndex
		ne.reply(nil)
	case entryUpdate:
		debug(r, "fms <- {", ne.typ, ne.index, "}")
		select {
		case <-r.shutdownCh:
			ne.reply(ErrServerClosed)
		case r.fsmTaskCh <- ne:
		}
	default:
		assert(false, "got unexpected entryType %d", ne.typ)
	}
}

// --------------------------------------------------------------------------

// todo: trace snapshot start and finish
func (r *Raft) takeSnapshot(t takeSnapshot) {
	// finally report back to raft, for log compaction
	var err error
	var meta SnapshotMeta
	defer func() {
		taken := snapTaken{req: t}
		taken.err = err
		taken.meta = meta
		r.snapTakenCh <- taken
	}()

	// get fsm state ---------------------------------------
	req := fsmSnapReq{task: newTask(), index: t.lastSnapIndex + t.threshold}
	r.fsmTaskCh <- req
	select {
	case <-r.shutdownCh:
		err = ErrServerClosed
		return
	case <-req.Done():
	}
	if req.Err() != nil {
		err = req.Err()
		return
	}
	resp := req.Result().(fsmSnapResp)
	defer resp.state.Release()

	// write snapshot to storage
	debug(r.id, "takingSnap:", resp.index)
	sink, err := r.snapshots.New(resp.index, resp.term, t.config)
	if err != nil {
		debug(r, "snapshots.New failed", err)
		err = opError(err, "Snapshots.New")
		return
	}
	err = resp.state.WriteTo(sink)
	meta, doneErr := sink.Done(err)
	if err != nil {
		debug(r, "FSMState.WriteTo failed", resp.index, err)
		err = opError(err, "FSMState.WriteTo")
		return
	}
	if err == nil && doneErr != nil {
		debug(r, "FSMState.Done failed", resp.index, err)
		err = opError(err, "FSMState.Done")
		return
	}
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

	// compact log
	// todo: we can check repl status and accordingly decide how much to delete
	metaIndexExists := t.meta.Index > r.snapIndex && t.meta.Index <= r.lastLogIndex
	if metaIndexExists {
		if err := r.storage.deleteLTE(t.meta); err != nil {
			if r.trace.Error != nil {
				r.trace.Error(err) // todo: should we reply err to user ?
			}
		}
	}
	t.req.reply(t.meta)
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

func (r *Raft) restoreFSMFromSnapshot() error {
	req := fsmRestoreReq{task: newTask()}
	r.fsmTaskCh <- req
	<-req.Done()
	if req.Err() != nil {
		return req.Err()
	}
	r.commitIndex, r.lastApplied = r.snapIndex, r.snapIndex
	return nil
}

// raft(onRestart/onInstallSnapReq) -> fsmLoop
type fsmRestoreReq struct {
	*task
}
