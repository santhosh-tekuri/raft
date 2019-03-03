package raft

// todo:
//
// * add OpError, use for storage/fsm/repl failures
// * rpc.resp: add error field. opError/rejectError/others(network etc)
// * trace: add onOpError
// * defaultTrace: add delegate
// * info: add func isOK() error
// * repl: on OpError, treat them as unreachable
