package raft

// todo:
//
// * add writeUint64s
// * export round
// * add OpError, use for storage/fsm/repl failures
// * rpc.resp: add error field. opError/rejectError/others(network etc)
// * trace: add onOpError
// * defaultTrace: add delegate
// * info: add func isOK() error
// * repl: on OpError, treat them as unreachable
// * candShip.requestVote goroutine should be added to wg
// * ErrConfigInProgress should contains configs
// * ErrCommitNotReady should tell how many entries it is behind to become commit ready
// * Snapshots add recover support
// * resolver should catch latest resolved addr
// * leader should back pressure on newEntryCh
