package raft

import (
	"container/list"
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

// if commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
//
// in case of leader:
// 		- newEntries is not nil
//      - reply end user with response
func (r *Raft) applyCommitted(newEntries *list.List) {
	// first commit latest config if not yet committed
	if !r.configs.IsCommitted() && r.configs.Latest.Index <= r.commitIndex {
		r.commitConfig()
		if r.state == Leader && !r.configs.Latest.isVoter(r.id) {
			debug(r, "leader -> follower notVoter")
			r.state = Follower
			r.leader = ""
		}
		// todo: we can provide option to shutdown
		//       if it is no longer part of new config
	}

	for {
		// send query/barrier entries to fsm
		if newEntries != nil {
			for newEntries.Len() > 0 {
				elem := newEntries.Front()
				ne := elem.Value.(NewEntry)
				if ne.index == r.lastApplied+1 && (ne.typ == entryQuery || ne.typ == entryBarrier) {
					newEntries.Remove(elem)
					debug(r, "fms <- {", ne.typ, ne.index, "}")
					select {
					case <-r.shutdownCh:
						ne.reply(ErrServerClosed)
						return
					case r.fsmTaskCh <- ne:
					}
				} else {
					break
				}
			}
		}

		if r.lastApplied+1 > r.commitIndex {
			return
		}

		// get lastApplied+1 entry
		var ne NewEntry
		if newEntries != nil && newEntries.Len() > 0 {
			elem := newEntries.Front()
			if elem.Value.(NewEntry).index == r.lastApplied+1 {
				ne = newEntries.Remove(elem).(NewEntry)
			}
		}
		if ne.entry == nil {
			ne.entry = &entry{}
			r.storage.getEntry(r.lastApplied+1, ne.entry)
		}

		// apply the entry
		switch ne.typ {
		case entryNop:
			// do nothing
		case entryConfig:
			// we already processed in beginning
			ne.reply(nil)
		case entryUpdate:
			debug(r, "fms <- {", ne.typ, ne.index, "}")
			select {
			case <-r.shutdownCh:
				ne.reply(ErrServerClosed)
				return
			case r.fsmTaskCh <- ne:
			}
		default:
			assert(true, "got unexpected entryType %d", ne.typ)
		}
		r.lastApplied++
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
