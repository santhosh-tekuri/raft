package raft

// todo:
//
// * add writeUint64s
// * defaultTrace: add delegate
// * info: add func isOK() error
// * candShip.requestVote goroutine should be added to wg
// * ErrConfigInProgress should contains configs
// * ErrCommitNotReady should tell how many entries it is behind to become commit ready
// * Snapshots add recover support
// * resolver should catch latest resolved addr
// * leader should back pressure on newEntryCh
// * can we provide type safe tasks: task.Result() now returns interface{}
