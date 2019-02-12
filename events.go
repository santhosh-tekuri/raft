package raft

var stateChanged = func(r *Raft) {}
var fsmApplied = func(r *Raft, index uint64) {}
