package raft

// todo: defaultTrace: add delegate
// todo: info: add func isOK() error
// todo: ErrConfigInProgress should contains configs
// todo: ErrCommitNotReady should tell how many entries it is behind to become commit ready
// todo: Snapshots add recover support
// todo: resolver should catch latest resolved addr
// todo: leader should back pressure on newEntryCh
// todo: can we provide type safe tasks: task.Result() now returns interface{}

// todo: leader can do log.sync just before updating commitIndex
// todo: maintain log.syncIndex and avoid log.sync if possible
//       note that on log.removeGTE update log.syncIndex
