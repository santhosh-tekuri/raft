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
				// send to trace
				t.reply(err)
				continue
			}
			if err = r.fsm.RestoreFrom(sr); err != nil {
				debug(r, "fsm.restore failed", err)
				// send to trace
				t.reply(err)
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
	var err error
	var meta SnapshotMeta
	defer func() {
		taken := snapTaken{req: t}
		taken.err = err
		taken.meta = meta
		r.snapTakenCh <- taken
	}()

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

	debug(r.id, "takingSnap:", resp.index)
	sink, err := r.snapshots.New(resp.index, resp.term, t.config)
	if err != nil {
		debug(r, "snapshots.New failed", err)
		// send to trace
		return
	}
	err = resp.state.WriteTo(sink)
	meta, doneErr := sink.Done(err)
	if err != nil {
		debug(r, "FSMState.WriteTo failed", resp.index, err)
		// send to trace
		return
	}
	if doneErr != nil {
		debug(r, "FSMState.Done failed", resp.index, err)
		// send to trace
		err = doneErr
		return
	}
}

func (r *Raft) onSnapshotTaken(t snapTaken) {
	r.snapTakenCh = nil // clear in progress flag

	if t.err != nil {
		t.req.reply(t.err)
		return
	}

	// compact log
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
