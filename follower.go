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

type follower struct {
	*Raft
	electionAborted bool
}

func (f *follower) init() {
	f.timer.reset(f.rtime.duration(f.hbTimeout))
	f.electionAborted = false
}

func (f *follower) release() {}

func (f *follower) resetTimer() {
	if yes, _ := f.canStartElection(); yes {
		f.electionAborted = false
		f.timer.reset(f.rtime.duration(f.hbTimeout))
	}
}

func (f *follower) onTimeout() {
	if trace {
		println(f, "heartbeatTimeout leader:", f.leader)
	}
	f.setLeader(0)
	if can, reason := f.canStartElection(); !can {
		f.electionAborted = true
		if trace {
			println(f, "electionAborted", reason)
		}
		if f.trace.ElectionAborted != nil {
			f.trace.ElectionAborted(f.liveInfo(), reason)
		}
		return
	}
	f.setState(Candidate)
}

func (f *follower) canStartElection() (can bool, reason string) {
	if f.configs.IsBootstrap() {
		return false, "not bootstrapped yet"
	}
	n, ok := f.configs.Latest.Nodes[f.nid]
	if !ok {
		return false, "not part of cluster"
	}
	if !n.Voter {
		return false, "not voter"
	}
	return true, ""
}
