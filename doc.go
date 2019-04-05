// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// todo:
//  - replication send as many entries as it has
//  - follower send one resp per batch of maxAppendEntries
//  - last resp should be success with lastIndex==req.lastIndex
//    or it sould be !success
//  - pipeline writer should notify pipeline reader about req.lastIndex
//    before writing entries, so that reader can read. otherwise writer
//    might take long time to finish writing
