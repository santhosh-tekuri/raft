package raft

import (
	"sync/atomic"
	"time"
)

type leaderUpdate struct {
	lastIndex, commitIndex uint64
}

type replication struct {
	member           *member
	connPool         *connPool
	storage          *storage
	heartbeatTimeout time.Duration
	conn             *netConn

	// index of the next log entry to send to that server
	// initialized to leader last log index + 1
	nextIndex uint64

	// index of highest log entry known to be replicated on server
	// initialized to 0, increases monotonically
	matchIndex uint64

	// leader notifies replication with update
	leaderUpdateCh chan leaderUpdate

	matchUpdatedCh chan<- *replication
	newTermCh      chan<- uint64
	stopCh         chan struct{}

	str string // used for debug() calls
}

const maxAppendEntries = 64 // todo: should be configurable

func (repl *replication) runLoop(req *appendEntriesRequest) {
	defer func() {
		if repl.conn != nil {
			repl.connPool.returnConn(repl.conn)
		}
	}()

	if repl.member.addr == req.leaderID {
		// self replication: when leaderUpdate comes
		// just notify that it is replicated
		repl.setMatchIndex(req.prevLogIndex)
		for {
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.leaderUpdateCh:
				repl.setMatchIndex(update.lastIndex)
			}
		}
	}

	ldrLastIndex, matchIndex := req.prevLogIndex, repl.getMatchIndex()
	debug(repl, "repl.start ldrLastIndex:", ldrLastIndex, "matchIndex:", matchIndex, "nextIndex:", repl.nextIndex)

	for {
		// prepare request ----------------------------
		var n uint64 // number of entries to be sent
		if matchIndex+1 == repl.nextIndex {
			n = ldrLastIndex - matchIndex // number of entries to be sent
			if n > maxAppendEntries {
				n = maxAppendEntries
			}
		}
		if repl.nextIndex == 1 {
			req.prevLogIndex, req.prevLogTerm = 0, 0
		} else if repl.nextIndex-1 == ldrLastIndex {
			req.prevLogIndex, req.prevLogTerm = ldrLastIndex, req.term
		} else {
			prevEntry := &entry{}
			repl.storage.getEntry(repl.nextIndex-1, prevEntry)
			req.prevLogIndex, req.prevLogTerm = prevEntry.index, prevEntry.term
		}

		if n == 0 {
			req.entries = nil
		} else {
			req.entries = make([]*entry, n)
			for i := range req.entries {
				req.entries[i] = &entry{}
				repl.storage.getEntry(repl.nextIndex+uint64(i), req.entries[i])
			}
		}

		// send request ----------------------------------
		var failures uint64
		for {
			resp, err := repl.appendEntries(req)
			if err != nil {
				failures++
				select {
				case <-repl.stopCh:
					return
				case <-time.After(backoff(failures)):
					continue
				}
			}

			// process response ------------------------------
			if resp.term > req.term {
				select {
				case <-repl.stopCh:
				case repl.newTermCh <- resp.term:
				}
				return
			}
			if resp.success {
				old := matchIndex
				if len(req.entries) == 0 {
					matchIndex = req.prevLogIndex
				} else {
					matchIndex = resp.lastLogIndex
					repl.nextIndex = resp.lastLogIndex + 1
				}
				if matchIndex != old {
					debug(repl, "matchIndex:", matchIndex)
					repl.setMatchIndex(matchIndex)
				}
			} else {
				if matchIndex+1 != repl.nextIndex {
					repl.nextIndex = max(min(repl.nextIndex-1, resp.lastLogIndex+1), 1)
					debug(repl, "nextIndex:", repl.nextIndex)
				} else {
					panic("faulty raft node") // todo: notify leader, that we stopped and dont panic
				}
			}
			break
		}

		if matchIndex == ldrLastIndex {
			// nothing to replicate. start heartbeat timer
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.leaderUpdateCh:
				ldrLastIndex, req.leaderCommitIndex = update.lastIndex, update.commitIndex
				debug(repl, "{last:", ldrLastIndex, "commit:", req.leaderCommitIndex, "} <-leaderUpdateCh")
			case <-afterRandomTimeout(repl.heartbeatTimeout / 10):
			}
		} else {
			// check signal if any, without blocking
			select {
			case <-repl.stopCh:
				return
			case update := <-repl.leaderUpdateCh:
				ldrLastIndex, req.leaderCommitIndex = update.lastIndex, update.commitIndex
				debug(repl, "{last:", ldrLastIndex, "commit:", req.leaderCommitIndex, "} <-leaderUpdateCh")
			default:
			}
		}
	}
}

func (repl *replication) getMatchIndex() uint64 {
	return atomic.LoadUint64(&repl.matchIndex)
}

func (repl *replication) setMatchIndex(v uint64) {
	atomic.StoreUint64(&repl.matchIndex, v)
	select {
	case <-repl.stopCh:
	case repl.matchUpdatedCh <- repl:
	}
}

func (repl *replication) appendEntries(req *appendEntriesRequest) (*appendEntriesResponse, error) {
	if repl.conn == nil {
		conn, err := repl.connPool.getConn()
		if err != nil {
			return nil, err
		}
		repl.conn = conn
	}
	resp := new(appendEntriesResponse)
	err := repl.conn.doRPC(rpcAppendEntries, req, resp)
	repl.member.contactSucceeded(err == nil)
	if err != nil {
		_ = repl.conn.close()
		repl.conn = nil
	}
	return resp, err
}

func (repl *replication) String() string {
	return repl.str
}
