package raft

// todo: add writeUint64s
// todo: defaultTrace: add delegate
// todo: info: add func isOK() error
// todo: ErrConfigInProgress should contains configs
// todo: ErrCommitNotReady should tell how many entries it is behind to become commit ready
// todo: Snapshots add recover support
// todo: resolver should catch latest resolved addr
// todo: leader should back pressure on newEntryCh
// todo: can we provide type safe tasks: task.Result() now returns interface{}

// todo: rpc.onAppendEntries, send reply and then start applying to fsm

// todo: we are backing off too much, meanwhile if we receive votereq, the repl should
//       wakeup and send appendEntries heartbeat immediately
