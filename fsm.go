package raft

import (
	"container/list"
)

type FSM interface {
	Apply(cmd []byte) interface{}
}

func (r *Raft) fsmLoop() {
	defer r.wg.Done()
	for ne := range r.fsmApplyCh {
		debug(r.addr, "fsm.apply", ne.index)
		ne.task.reply(r.fsm.Apply(ne.entry.data))
		fsmApplied(r, ne.index) // generate event
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

		if newEntries == nil {
			ne.entry = &entry{}
			r.storage.getEntry(r.lastApplied+1, ne.entry)
		} else {
			elem := newEntries.Front()
			ne = elem.Value.(newEntry)
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
