package raft

import (
	"container/list"
	"errors"
	"io"
)

var ErrSnapshotThreshold = errors.New("raft: not enough outstanding logs to snapshot")
var ErrNoUpdates = errors.New("raft: no updates to FSM")

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
	for {
		select {
		case <-r.shutdownCh:
			debug(r, "fsmLoop shutdown")
			return
		case t := <-r.fsmTaskCh:
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
				sr, err := r.snapshots.Open(t.index)
				if err != nil {
					debug(r, "snapshots.Open failed", t.index, err)
					// send to trace
					t.reply(err)
					continue
				}
				defer sr.Close()
				if err = r.fsm.RestoreFrom(sr); err != nil {
					debug(r, "fsm.restore failed", t.index, err)
					// send to trace
					t.reply(err)
					continue
				}
				meta, err := r.snapshots.Meta(t.index)
				if err != nil {
					debug(r, "snapshots.Meta failed", t.index, err)
					// send to trace
					t.reply(err)
					continue
				}
				updateIndex, updateTerm = t.index, meta.Term
				t.reply(nil)
				debug(r, "restored snapshot", t.index)
			}
		}
	}
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
			r.stateChanged()
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
						return
					case r.fsmTaskCh <- ne:
					}
				} else {
					break
				}
			}
		}

		if r.commitIndex > r.lastApplied {
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
				r.log.getEntry(r.lastApplied+1, ne.entry)
			}

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
					return
				case r.fsmTaskCh <- ne:
				}
			default:
				assert(true, "got unexpected entryType %d", ne.typ)
			}
			r.lastApplied++
		} else {
			return
		}
	}
}

// --------------------------------------------------------------------------

// todo: trace snapshot start and finish
func (r *Raft) snapLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.shutdownCh:
			debug(r, "snapLoop shutdown")
			return
		case t := <-r.snapTaskCh:
			r.takeSnapshot(t)
		}
	}
}

func (r *Raft) takeSnapshot(t takeSnapshot) {
	var req fsmSnapReq
	var meta SnapshotMeta
	var err error
	defer func() {
		if err != nil {
			t.reply(err)
		} else if req.Err() != nil {
			t.reply(req.Err())
		} else {
			t.reply(meta)
		}
	}()

	snaps, err := r.snapshots.List()
	if err != nil {
		return
	}
	latestSnap := uint64(0)
	if len(snaps) > 0 {
		latestSnap = snaps[0]
	}
	req = fsmSnapReq{task: newTask(), index: latestSnap + t.threshold}
	r.fsmTaskCh <- req
	select {
	case <-r.shutdownCh:
		err = ErrServerClosed
		return
	case <-req.Done():
	}
	if req.Err() != nil {
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
	err = r.log.deleteLTE(resp.index)
}
