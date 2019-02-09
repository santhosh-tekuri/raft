package raft

import (
	"container/list"
	"fmt"
)

type FSM interface {
	Apply(cmd []byte) interface{}
}

// if commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
//
// in case of leader:
// 		- newEntries is not nil
//      - reply end user with response
func (r *Raft) fsmApply(newEntries *list.List) {
	for r.commitIndex > r.lastApplied {
		entry := &entry{}
		r.storage.getEntry(r.lastApplied+1, entry)

		// apply to fsm
		var resp interface{}
		debug(r, "applying cmd", entry.index)
		if entry.typ != entryNoop {
			resp = r.fsm.Apply(entry.data)
		}
		r.lastApplied++

		if newEntries != nil {
			// reply end user
			elem := newEntries.Front()
			newEntry := elem.Value.(newEntry)
			if newEntry.index == entry.index {
				newEntry.sendResponse(resp)
				newEntries.Remove(elem)
			} else {
				panic(fmt.Sprintf("[BUG] got entry %d, want %d", newEntry.index, entry.index))
			}
		}
	}
}
