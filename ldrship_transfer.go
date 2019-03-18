package raft

import "time"

type transfer struct {
	transferLdr
	timer    *safeTimer
	deadline time.Time
	term     uint64
	rpcCh    <-chan rpcResponse
}

func (t transfer) inProgress() bool {
	return t.timer.active
}

func (t transfer) targetChosen() bool {
	return t.rpcCh != nil
}

func (t *transfer) reply(err error) {
	if t.timer.active {
		debug("reply leadership transfer:", err)
		t.task.reply(err)
	}
	t.timer.stop()
	t.rpcCh = nil
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
	l.transfer.deadline = time.Now().Add(t.timeout)
	l.transfer.timer.reset(t.timeout)
	if tgt := l.choseTransferTgt(); tgt != 0 {
		l.doTransfer(tgt)
	}
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

func (l *ldrShip) choseTransferTgt() uint64 {
	if l.transfer.target != 0 {
		f := l.flrs[l.transfer.target]
		if f.status.noContact.IsZero() && f.status.matchIndex == l.lastLogIndex {
			return l.transfer.target
		}
	} else {
		for id, n := range l.configs.Latest.Nodes {
			if id != l.nid && n.Voter {
				f := l.flrs[id]
				if f.status.noContact.IsZero() && f.status.matchIndex == l.lastLogIndex {
					return id
				}
			}
		}
	}
	return 0
}

func (l *ldrShip) doTransfer(target uint64) {
	debug(l, "transferring leadership:", target)
	pool := l.getConnPool(target)
	ch := make(chan rpcResponse, 1)
	l.transfer.rpcCh = ch
	req := &timeoutNowReq{req{l.term, l.nid}}
	debug(l, target, ">>", req)
	go pool.doRPC(req, &timeoutNowResp{}, l.transfer.deadline, ch)
}

func (l *ldrShip) onTransferTimeout() {
	l.transfer.reply(TimeoutError("transferLeadership"))
}

func (l *ldrShip) onTimeoutNowResult(rpc rpcResponse) {
	debug(l, rpc)
	l.transfer.rpcCh = nil
	if rpc.err != nil {
		f := l.flrs[rpc.from]
		if f.status.noContact.IsZero() {
			f.status.noContact = time.Now()
		}
		if l.transfer.target == 0 {
			if tgt := l.choseTransferTgt(); tgt != 0 {
				l.doTransfer(tgt)
			}
		}
	}
}
