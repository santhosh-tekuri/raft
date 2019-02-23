package raft

import "log"

var StateChanged = func(r *Raft, state State) {
	if state == Leader {
		log.Println("[INFO] raft: cluster leadership acquired")
	}
}
var ElectionAborted = func(r *Raft, reason string) {
	log.Printf("[INFO] raft: %s, aborting election", reason)
}
