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
		case ne := <-r.fsmApplyCh:
			debug(r.id, "fsm.apply", ne.typ, ne.index)
			var resp interface{}
			if ne.typ == entryUpdate || ne.typ == entryQuery {
				resp = r.fsm.Execute(ne.entry.data)
				if ne.typ == entryUpdate {
					updateIndex, updateTerm = ne.index, ne.term
				}
			}
			ne.task.reply(resp)
		case t := <-r.fsmSnapCh:
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
				t.reply(err)
				continue
			}
			t.index, t.term = updateIndex, updateTerm
			t.reply(state)
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
					case r.fsmApplyCh <- ne:
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
				case r.fsmApplyCh <- ne:
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
		req := &snapFSM{task: &task{done: make(chan struct{})}}
		select {
		case <-r.shutdownCh:
			debug(r, "snapLoop shutdown")
			return
		case t := <-r.userSnapCh:
			req.config = t.config
			snapIndex, err := r.takeSnapshot(req, t.threshold)
			if err != nil {
				t.reply(err)
			} else {
				t.reply(snapIndex)
			}
		}
	}
}

func (r *Raft) takeSnapshot(req *snapFSM, threshold uint64) (snapIndex uint64, err error) {
	snaps, err := r.snapshots.List()
	if err != nil {
		return 0, err
	}
	latestSnap := uint64(0)
	if len(snaps) > 0 {
		latestSnap = snaps[0]
	}
	req.index = latestSnap + threshold
	r.fsmSnapCh <- req
	select {
	case <-r.shutdownCh:
		return 0, ErrServerClosed
	case <-req.Done():
	}
	if req.Err() != nil {
		return 0, req.Err()
	}
	fsmState := req.Result().(FSMState)
	defer fsmState.Release()

	sink, err := r.snapshots.New(req.index, req.term, req.config)
	if err != nil {
		return 0, err
	}
	err = fsmState.WriteTo(sink)
	sink.Done(err)
	if err != nil {
		return 0, err
	}

	return req.index, r.log.deleteLTE(req.index)
}
