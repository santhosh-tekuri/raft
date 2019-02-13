package raft

var stateChanged = func(r *Raft) {}
var fsmApplied = func(r *Raft, index uint64) {}
var gotRequestVote = func(r *Raft, req *requestVoteRequest) {}
