package raft

import (
	"bufio"
	"bytes"
	"container/list"
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
	for t := range fsm.ch {
		debug(fsm, t)
		switch t := t.(type) {
		case fsmApply:
			fsm.onApply(t)
		case fsmSnapReq:
			fsm.onSnapReq(t)
		case fsmRestoreReq:
			fsm.onRestoreReq(t)
		}
	}
}

func (fsm *stateMachine) onApply(t fsmApply) {
	var elem *list.Element
	if t.newEntries != nil {
		elem = t.newEntries.Front()
	}
	commitIndex := t.log.LastIndex()
	for {
		// process non log entries
		for elem != nil {
			ne := elem.Value.(newEntry)
			if ne.index == fsm.index+1 && (ne.typ == entryRead || ne.typ == entryBarrier) {
				elem = elem.Next()
				debug(fsm, "fsm.apply", ne.typ, ne.index)
				var resp interface{}
				if ne.typ == entryRead {
					resp = fsm.Read(ne.data)
				}
				ne.reply(resp)
			} else {
				break
			}
		}

		if fsm.index+1 > commitIndex {
			return
		}

		var ne newEntry
		if elem != nil && elem.Value.(newEntry).index == fsm.index+1 {
			ne = elem.Value.(newEntry)
			elem = elem.Next()
		} else {
			b, err := t.log.Get(fsm.index + 1)
			if err != nil {
				panic(opError(err, "Log.Get(%d)", fsm.index+1))
			}
			ne.entry = &entry{}
			if err := ne.entry.decode(bytes.NewReader(b)); err != nil {
				panic(opError(err, "Log.Get(%d).decode", fsm.index+1))
			}
		}

		debug(fsm, "fsm.apply", ne.typ, ne.index)
		var resp interface{}
		if ne.typ == entryUpdate {
			resp = fsm.Update(ne.data)
		}
		ne.reply(resp)
		fsm.index, fsm.term = ne.index, ne.term
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
		debug(fsm, "fsm.Snapshot failed", err)
		// todo: send to trace
		t.reply(opError(err, "fsm.Snapshot"))
		return
	}
	t.reply(fsmSnapResp{
		index: fsm.index,
		term:  fsm.term,
		state: state,
	})
}

func (fsm *stateMachine) onRestoreReq(t fsmRestoreReq) {
	meta, sr, err := fsm.snaps.open()
	if err != nil {
		debug(fsm, "snapshots.open failed", err)
		t.reply(opError(err, "snapshots.open"))
		return
	}
	if err = fsm.RestoreFrom(sr); err != nil {
		debug(fsm, "fsm.restore failed", err)
		// todo: detect where err occurred in restoreFrom/sr.read
		t.reply(opError(err, "FSM.RestoreFrom"))
	} else {
		fsm.index, fsm.term = meta.Index, meta.Term
		debug(fsm, "restored snapshot", meta.Index)
		t.reply(nil)
	}
	_ = sr.Close()
}

type fsmApply struct {
	newEntries *list.List
	log        *log.Log
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
	fsm.ch <- req
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
	r.fsm.ch <- req
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
