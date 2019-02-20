package raft

import (
	"container/list"
)

type FSM interface {
	Apply(cmd []byte) interface{}
}

func (r *Raft) fsmLoop() {
	defer r.wg.Done()
	for newEntry := range r.fsmApplyCh {
		debug(r.addr, "fsm.apply", newEntry.index)
		resp := r.fsm.Apply(newEntry.entry.data)
		fsmApplied(r, newEntry.index) // generate event
		newEntry.sendResponse(resp)
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
		var ne NewEntry

		if newEntries == nil {
			ne.entry = &entry{}
			r.storage.getEntry(r.lastApplied+1, ne.entry)
		} else {
			elem := newEntries.Front()
			ne = elem.Value.(NewEntry)
			assert(ne.index == r.lastApplied+1, "BUG")
			newEntries.Remove(elem)
		}

		debug(r, "lastApplied", ne.index)
		if ne.entry.typ == entryCommand {
			select {
			case <-r.shutdownCh:
			case r.fsmApplyCh <- ne:
			}
		}
	}
}
