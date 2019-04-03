package raft

// todo: defaultTrace: add delegate
// todo: info: add func isOK() error
// todo: ErrConfigInProgress should contains configs
// todo: ErrCommitNotReady should tell how many entries it is behind to become commit ready
// todo: Snapshots add recover support
// todo: resolver should catch latest resolved addr
// todo: leader should back pressure on newEntryCh
// todo: can we provide type safe tasks: task.Result() now returns interface{}

// todo: nonvoter should not bother leader with matchIndex updates until round is completed

// todo:
//  nonvoter with none action, should not send matchIndex updates
//  in such case:
//     info.followers.matchIndex will not be reflected for them
//     promotion might require to fetch matchIndex first
