package raft

import (
	"container/list"
)

// todo: add availability methods
type FSM interface {
	Apply(cmd []byte) interface{}
}

func (r *Raft) fsmLoop() {
	defer r.wg.Done()
	for ne := range r.fsmApplyCh {
		debug(r.addr, "fsm.apply", ne.index)
		var resp interface{}
		if ne.entry.typ == entryCommand {
			resp = r.fsm.Apply(ne.entry.data)
		}
		ne.task.reply(resp)
	}
	debug(r, "fsmLoop shutdown")
}

// if commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
//
// in case of leader:
// 		- newEntries is not nil
//      - reply end user with response
func (r *Raft) fsmApply(newEntries *list.List) {
	for ; r.commitIndex > r.lastApplied; r.lastApplied++ {
		var ne newEntry

		// check if entry to be applied is user submitted
		// or we are applying old entry from storage
		if newEntries != nil {
			elem := newEntries.Front()
			if elem.Value.(newEntry).index == r.lastApplied+1 {
				ne = elem.Value.(newEntry)
				newEntries.Remove(elem)
			}
		}

		if ne.entry == nil {
			ne.entry = &entry{}
			r.storage.getEntry(r.lastApplied+1, ne.entry)
		}

		debug(r, "lastApplied", ne.index)
		select {
		case <-r.shutdownCh:
		case r.fsmApplyCh <- ne:
		}
	}
}
