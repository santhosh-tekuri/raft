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

// +build trace

package raft

import (
	"fmt"
	"net"
	"strings"
	"time"
)

const (
	trace = true
)

var traceDone = make(chan struct{})
var traceCh = make(chan string, 10000)

func init() {
	go func() {
		for msg := range traceCh {
			if isBarrier(msg) {
				close(traceDone)
				continue
			}
			fmt.Print(msg)
		}
	}()
}

var boot = time.Now()

func isBarrier(msg string) bool {
	return strings.HasSuffix(msg, "barrier\n")
}

func println(args ...interface{}) {
	// uncomment this to print only debug lines with first argument "xxx"
	// this is useful for to print only specific debug lines

	//if len(args) == 0 || args[0] != "xxx" {
	//	return
	//}

	var msg string
	if len(args) == 0 {
		msg = "\n"
	} else {
		ms := time.Now().Sub(boot).Nanoseconds() / 1e6
		msg = fmt.Sprintln(append([]interface{}{ms}, args...)...)
	}
	traceCh <- msg
	if isBarrier(msg) {
		// wait all pending traceCh are printed
		<-traceDone
		traceDone = make(chan struct{})
	}
}

// Stringers ----------------------------------------------------------

func (resp resp) String() string {
	if resp.result == unexpectedErr {
		return fmt.Sprintf("T%d %s %v", resp.term, resp.result, resp.err)
	}
	return fmt.Sprintf("T%d %s", resp.term, resp.result)
}

func (req *identityReq) String() string {
	format := "identityReq{T%d M%d C%d M%d}"
	return fmt.Sprintf(format, req.term, req.src, req.cid, req.nid)
}

func (resp *identityResp) String() string {
	return fmt.Sprintf("identityResp{%v}", resp.resp)
}

func (req *voteReq) String() string {
	format := "voteReq{T%d M%d last:(%d,%d) transfer:%v}"
	return fmt.Sprintf(format, req.term, req.src, req.lastLogIndex, req.lastLogTerm, req.transfer)
}

func (resp *voteResp) String() string {
	return fmt.Sprintf("voteResp{%s}", resp.resp)
}

func (req *appendReq) String() string {
	format := "appendReq{T%d M%d prev:(%d,%d), #entries: %d, commit:%d}"
	return fmt.Sprintf(format, req.term, req.src, req.prevLogIndex, req.prevLogTerm, req.numEntries, req.ldrCommitIndex)
}

func (resp *appendResp) String() string {
	format := "appendResp{%v last:%d}"
	return fmt.Sprintf(format, resp.resp, resp.lastLogIndex)
}

func (req *installSnapReq) String() string {
	format := "installSnapReq{T%d M%d last:(%d,%d), size:%d}"
	return fmt.Sprintf(format, req.term, req.src, req.lastIndex, req.lastIndex, req.size)
}

func (resp *installSnapResp) String() string {
	return fmt.Sprintf("installSnapResp{%v}", resp.resp)
}

func (req *timeoutNowReq) String() string {
	return fmt.Sprintf("timeoutNowReq{T%d M%d}", req.term, req.src)
}

func (resp *timeoutNowResp) String() string {
	return fmt.Sprintf("timeoutNowResp{%v}", resp.resp)
}

func (n Node) String() string {
	return fmt.Sprintf("M%d", n.ID)
}

//go:norace
func (r *Raft) String() string {
	return fmt.Sprintf("M%d %d %d %d %s |", r.nid, r.term, r.commitIndex, r.lastLogIndex, string(r.state))
}

//go:norace
func (r *replication) String() string {
	return fmt.Sprintf("M%d %d %d %d R |", r.node.ID, r.matchIndex, r.nextIndex, r.ldrLastIndex)
}

func (fsm *stateMachine) String() string {
	return fmt.Sprintf("M%d %d %d FSM |", fsm.id, fsm.index, fsm.term)
}

func (u leaderUpdate) String() string {
	return fmt.Sprintf("leaderUpdate{last:%d, commit:%d, config: %v}", u.log.LastIndex(), u.commitIndex, u.config)
}

func (u replUpdate) String() string {
	id := u.status.id
	switch u := u.update.(type) {
	case matchIndex:
		return fmt.Sprintf("replUpdate{M%d matchIndex:%d}", id, u.val)
	case newTerm:
		return fmt.Sprintf("replUpdate{M%d newTerm:%d}", id, u.val)
	case noContact:
		if u.time.IsZero() {
			return fmt.Sprintf("replUpdate{M%d yesContact}", id)
		}
		return fmt.Sprintf("replUpdate{M%d noContact err:%v}", id, u.err)
	case removeLTE:
		return fmt.Sprintf("replUpdate{M%d removeLTE:%d}", id, u.val)
	case error:
		return fmt.Sprintf("replUpdate{M%d error:%v}", id, u)
	default:
		return fmt.Sprintf("replUpdate{M%d %T}", id, u)
	}
}

func (ne *newEntry) String() string {
	switch ne.typ {
	case entryUpdate:
		return fmt.Sprintf("update{%s}", string(ne.data))
	case entryRead:
		return fmt.Sprintf("read{%s}", string(ne.data))
	case entryBarrier:
		return "barrier"
	default:
		return fmt.Sprintf("%#v", ne)
	}
}

func (t changeConfig) String() string {
	return fmt.Sprintf("changeConfig{%s}", t.newConf)
}

func (t waitForStableConfig) String() string {
	return "WaitForStableConfig{}"
}

func (t takeSnapshot) String() string {
	return fmt.Sprintf("takeSnapshot{%d}", t.threshold)
}

func (t transferLdr) String() string {
	if t.target == 0 {
		return fmt.Sprintf("transferLdr{%s}", t.timeout)
	}
	return fmt.Sprintf("transferLdr{M%d %s}", t.target, t.timeout)
}

func (r rpcResponse) String() string {
	if r.err == nil {
		return fmt.Sprintf("M%d << %s", r.from, r.response)
	}
	return fmt.Sprintf("M%d << %T{} err: %v", r.from, r.response, r.err)
}

func (t fsmApply) String() string {
	var newEntries string
	if t.neHead != nil {
		first := t.neHead.index
		var last uint64
		for ne := t.neHead; ne != nil; ne = ne.next {
			last = ne.index
		}
		newEntries = fmt.Sprintf(", newEntries[%d..%d]", first, last)
	}
	return fmt.Sprintf("fsmApply{commitIndex:%d%s}", t.log.LastIndex(), newEntries)
}

func (t fsmSnapReq) String() string {
	return fmt.Sprintf("fsmSnapReq{index:%d}", t.index)
}

func (t fsmRestoreReq) String() string {
	return "fsmRestoreReq{}"
}

func (t lastApplied) String() string {
	return "lastApplied{}"
}

func (t rpcType) String() string {
	switch t {
	case rpcIdentity:
		return "identity"
	case rpcVote:
		return "vote"
	case rpcAppendEntries:
		return "append"
	case rpcInstallSnap:
		return "installSnap"
	case rpcTimeoutNow:
		return "timeoutNow"
	}
	return fmt.Sprintf("rpcType(%d)", int(t))
}

func (p *connPool) String() string {
	return fmt.Sprintf("M%d connPool M%d", p.src, p.nid)
}

func (s *server) String() string {
	host, _, _ := net.SplitHostPort(s.lr.Addr().String())
	return fmt.Sprintf("%s Server", host)
}
