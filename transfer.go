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

import (
	"fmt"
	"time"
)

type transfer struct {
	// transfer task submitted by user. should be used
	// only when timer.active==true
	transferLdr

	// started when transfer request received, with the transferLdr.timeout
	// this timer is inactive no transfer task is in progress
	timer    *safeTimer
	deadline time.Time

	// current term when transfer request received
	term uint64

	// channel to receive timeoutNowResp
	//
	// non nil, only when timeoutNowReq is sent,
	// and response not yet received
	respCh chan rpcResponse

	// after we got timeoutNowResp success, we expect target
	// to start election. There is a chance that target's
	// network broken just before sending votReq. This timer
	// helps to try another target, in such cases.
	//
	// on timeout, we try another target
	newTermTimer *safeTimer
}

func (t transfer) inProgress() bool {
	return t.timer.active
}

func (t transfer) targetChosen() bool {
	return t.respCh != nil || t.newTermTimer.active
}

func (t *transfer) reply(err error) {
	if trace {
		println("transferLdr.reply err:", err)
	}
	t.task.reply(err)
	t.timer.stop()
	t.respCh = nil
	t.newTermTimer.stop()
}

// ----------------------------------------------------

func (l *leader) onTransfer(t transferLdr) {
	if trace {
		println(l, "got", t)
	}
	if err := l.validateTransfer(t); err != nil {
		if trace {
			println(l, "transferLdr invalid:", err)
		}
		t.reply(err)
		return
	}
	l.transfer.term = l.term
	l.transfer.transferLdr = t
	if t.timeout <= 0 {
		t.timeout = 2 * l.hbTimeout
	}
	l.transfer.timer.reset(t.timeout)
	l.transfer.deadline = time.Now().Add(t.timeout)
	l.tryTransfer()
}

func (l *leader) validateTransfer(t transferLdr) error {
	if l.transfer.inProgress() {
		return InProgressError("transferLeadership")
	}
	if l.configs.Latest.numVoters() == 1 {
		return ErrTransferNoVoter
	}
	if t.target != 0 {
		if t.target == l.nid {
			return ErrTransferSelf
		}
		if n, ok := l.configs.Latest.Nodes[t.target]; ok {
			if !n.Voter {
				return ErrTransferTargetNonvoter
			}
		} else {
			return ErrTransferInvalidTarget
		}
	}
	return nil
}

func (l *leader) tryTransfer() {
	// chose ready target
	var target uint64
	if l.transfer.target != 0 {
		if l.configs.Latest.isVoter(l.transfer.target) {
			repl := l.repls[l.transfer.target]
			if repl.status.noContact.IsZero() && repl.status.matchIndex == l.lastLogIndex {
				target = l.transfer.target
			}
		}
	} else {
		for id, n := range l.configs.Latest.Nodes {
			if id != l.nid && n.Voter {
				repl := l.repls[id]
				if repl.status.noContact.IsZero() && repl.status.matchIndex == l.lastLogIndex {
					target = id
					break
				}
			}
		}
	}

	if target != 0 {
		l.transfer.respCh = make(chan rpcResponse, 1)
		req := &timeoutNowReq{req{l.term, l.nid}}
		if trace {
			println(l, target, ">>", req)
		}
		pool := l.getConnPool(target)
		go func(ch chan<- rpcResponse, deadline time.Time) {
			resp := &timeoutNowResp{}
			err := pool.doRPC(req, resp, deadline)
			ch <- rpcResponse{resp, pool.nid, err}
		}(l.transfer.respCh, l.transfer.deadline)
	}
}

func (l *leader) onTransferTimeout() {
	l.replyTransfer(TimeoutError("transferLeadership"))
}

func (l *leader) replyTransfer(err error) {
	l.transfer.reply(err)
	l.checkConfigActions(nil, l.configs.Latest)
}

func (l *leader) onTimeoutNowResult(rpc rpcResponse) {
	if trace {
		println(l, rpc)
	}
	l.transfer.respCh = nil
	if rpc.err != nil {
		repl := l.repls[rpc.from]
		if repl.status.noContact.IsZero() {
			repl.status.noContact = time.Now()
			repl.status.err = rpc.err
		}
		if l.transfer.target == 0 {
			l.tryTransfer()
		}
		return
	}
	if rpc.response.getResult() != success {
		if l.transfer.target == 0 {
			l.replyTransfer(fmt.Errorf("raft.transferLeadership: target rejected with %v", rpc.response.getResult()))
			return
		}
		l.tryTransfer()
		return
	}
	// todo: make configurable, min 60ms required for persisting term and self-vote to disk
	l.transfer.newTermTimer.reset(200 * time.Millisecond)
}

func (l *leader) onNewTermTimeout() {
	l.tryTransfer()
}
