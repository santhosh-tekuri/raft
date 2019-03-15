package raft

import "time"

func (l *ldrShip) onTransfer(t transferLdr) {
	debug(l, "got", t)
	if err := l.validateTransfer(t); err != nil {
		debug(l, "transferLdr rejected:", err)
		t.reply(err)
		return
	}

	t.term = l.term
	l.transferLdr = t
	l.transferTimer.reset(t.timeout)
	if tgt := l.choseTransferTgt(); tgt != 0 {
		l.doTransfer(tgt)
	}
}

func (l *ldrShip) validateTransfer(t transferLdr) error {
	if l.transferTimer.active {
		return InProgressError("transferLeadership")
	}
	if l.configs.Latest.numVoters() == 1 {
		return ErrLeadershipTransferNoVoter
	}
	if t.target != 0 {
		if t.target == l.id {
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
	if l.transferLdr.target != 0 {
		f := l.flrs[l.transferLdr.target]
		if f.status.noContact.IsZero() && f.status.matchIndex == l.lastLogIndex {
			return l.transferLdr.target
		}
	} else {
		for id, n := range l.configs.Latest.Nodes {
			if id != l.id && n.Voter {
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
	ch := make(chan timeoutNowResult, 1)
	l.transferLdr.rpcCh = ch
	req := &timeoutNowReq{l.term, l.id}
	go func() {
		conn, err := pool.getConn()
		if err != nil {
			ch <- timeoutNowResult{target: pool.id, err: err}
			return
		}
		resp := new(timeoutNowResp)
		debug(l.id, ">>", req)
		if l.trace.sending != nil {
			l.trace.sending(l.id, pool.id, Leader, req)
		}
		_ = conn.conn.SetDeadline(time.Now().Add(5 * l.hbTimeout)) // todo
		if err = conn.doRPC(req, resp); err != nil {
			_ = conn.close()
			ch <- timeoutNowResult{target: pool.id, err: err}
			return
		}
		pool.returnConn(conn)
		debug(l.id, "<<", resp)
		if l.trace.received != nil {
			l.trace.received(l.id, pool.id, Leader, req.term, resp)
		}
		ch <- timeoutNowResult{target: pool.id, err: nil}
	}()
}

func (l *ldrShip) onTransferTimeout() {
	l.replyTransfer(TimeoutError("transferLeadership"))
}

func (l *ldrShip) onTimeoutNowResult(result timeoutNowResult) {
	l.transferLdr.rpcCh = nil
	if result.err != nil {
		f := l.flrs[result.target]
		if f.status.noContact.IsZero() {
			f.status.noContact = time.Now()
		}
		if l.transferLdr.target == 0 {
			if tgt := l.choseTransferTgt(); tgt != 0 {
				l.doTransfer(tgt)
			}
		}
	}
}

func (l *ldrShip) replyTransfer(err error) {
	if l.transferTimer.active {
		debug(l, "reply leadership transfer:", err)
		l.transferLdr.reply(err)
	}
	l.transferTimer.stop()
	l.transferLdr.rpcCh = nil
}
