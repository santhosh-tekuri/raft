package raft

import "time"

type transfer struct {
	// transfer task submitted by user. should be used
	// only when timer.active==true
	transferLdr

	// started when transfer request received, with the transferLdr.timeout
	// this timer is inactive no transfer task is in progress
	timer *safeTimer

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
	if t.timer.active {
		debug("reply leadership transfer:", err)
		t.task.reply(err)
	}
	t.timer.stop()
	t.respCh = nil
	t.newTermTimer.stop()
}

// ----------------------------------------------------

func (l *ldrShip) onTransfer(t transferLdr) {
	debug(l, "got", t)
	if err := l.validateTransfer(t); err != nil {
		debug(l, "transferLdr rejected:", err)
		t.reply(err)
		return
	}

	l.transfer.term = l.term
	l.transfer.transferLdr = t
	l.transfer.timer.reset(t.timeout)
	l.tryTransfer()
}

func (l *ldrShip) validateTransfer(t transferLdr) error {
	if l.transfer.inProgress() {
		return InProgressError("transferLeadership")
	}
	if l.configs.Latest.numVoters() == 1 {
		return ErrLeadershipTransferNoVoter
	}
	if t.target != 0 {
		if t.target == l.nid {
			return ErrLeadershipTransferSelf
		}
		if n, ok := l.configs.Latest.Nodes[t.target]; ok {
			if !n.Voter {
				return ErrLeadershipTransferTargetNonvoter
			}
		} else {
			return ErrLeadershipTransferInvalidTarget
		}
	}
	return nil
}

func (l *ldrShip) tryTransfer() {
	// chose ready target
	var target uint64
	if l.transfer.target != 0 {
		f := l.flrs[l.transfer.target]
		if f.status.noContact.IsZero() && f.status.matchIndex == l.lastLogIndex {
			target = l.transfer.target
		}
	} else {
		for id, n := range l.configs.Latest.Nodes {
			if id != l.nid && n.Voter {
				f := l.flrs[id]
				if f.status.noContact.IsZero() && f.status.matchIndex == l.lastLogIndex {
					target = id
					break
				}
			}
		}
	}

	if target != 0 {
		debug(l, "transferring leadership:", target)
		pool := l.getConnPool(target)
		l.transfer.respCh = make(chan rpcResponse, 1)
		req := &timeoutNowReq{req{l.term, l.nid}}
		debug(l, target, ">>", req)
		go func(ch chan<- rpcResponse) {
			resp := &timeoutNowResp{}
			err := pool.doRPC(req, resp)
			ch <- rpcResponse{resp, pool.nid, err}
		}(l.transfer.respCh)
	}
}

func (l *ldrShip) onTransferTimeout() {
	l.transfer.reply(TimeoutError("transferLeadership"))
	l.checkActions()
}

func (l *ldrShip) onTimeoutNowResult(rpc rpcResponse) {
	debug(l, rpc)
	l.transfer.respCh = nil
	if rpc.err != nil {
		f := l.flrs[rpc.from]
		if f.status.noContact.IsZero() {
			f.status.noContact = time.Now()
		}
		if l.transfer.target == 0 {
			l.tryTransfer()
		}
	} else {
		// todo: make configurable, min 60ms required for persisting term and self-vote to disk
		l.transfer.newTermTimer.reset(200 * time.Millisecond)
	}
}

func (l *ldrShip) onNewTermTimeout() {
	l.tryTransfer()
}
