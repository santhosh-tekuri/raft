package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type member struct {
	addr             string
	timeout          time.Duration
	heartbeatTimeout time.Duration

	connPoolMu sync.Mutex
	connPool   []*netConn
	maxConns   int

	nextIndex  uint64
	matchIndex uint64

	// owned exclusively by raft main goroutine
	// used to recalculateMatch
	matchedIndex uint64

	// leader notifies replicator when its lastLogIndex changes
	leaderLastIndexCh chan uint64

	// leader notifies replicator when its commitIndex changes
	leaderCommitIndexCh chan uint64
}

func (m *member) getConn() (*netConn, error) {
	m.connPoolMu.Lock()
	defer m.connPoolMu.Unlock()

	num := len(m.connPool)
	if num == 0 {
		return dial(m.addr, m.timeout)
	}
	var conn *netConn
	conn, m.connPool[num-1] = m.connPool[num-1], nil
	m.connPool = m.connPool[:num-1]
	return conn, nil
}

func (m *member) returnConn(conn *netConn) {
	m.connPoolMu.Lock()
	defer m.connPoolMu.Unlock()

	if len(m.connPool) < m.maxConns {
		m.connPool = append(m.connPool, conn)
	} else {
		conn.close()
	}
}

func (m *member) doRPC(typ rpcType, req, resp command) error {
	conn, err := m.getConn()
	if err != nil {
		return err
	}
	if err = conn.doRPC(typ, req, resp); err != nil {
		conn.close()
		return err
	}
	m.returnConn(conn)
	return nil
}

func (m *member) requestVote(req *requestVoteRequest) (*requestVoteResponse, error) {
	resp := new(requestVoteResponse)
	err := m.doRPC(rpcRequestVote, req, resp)
	return resp, err
}

func (m *member) appendEntries(req *appendEntriesRequest) (*appendEntriesResponse, error) {
	resp := new(appendEntriesResponse)
	err := m.doRPC(rpcAppendEntries, req, resp)
	return resp, err
}

func (m *member) retryAppendEntries(req *appendEntriesRequest, stopCh <-chan struct{}) (*appendEntriesResponse, error) {
	var failures uint64
	for {
		resp, err := m.appendEntries(req)
		if err != nil {
			failures++
			select {
			case <-time.After(backoff(failures)):
				continue
			case <-stopCh:
				return resp, err
			}
		}
		return resp, nil
	}
}

const maxAppendEntries = 64

func (m *member) replicate(storage *storage, req *appendEntriesRequest, matchUpdatedCh chan<- *member, stopCh <-chan struct{}) {
	ldr := fmt.Sprintf("%s %d %s |", req.leaderID, req.term, leader)

	lastIndex, matchIndex := req.prevLogIndex, m.matchIndex

	// know which entries to replicate: fixes m.nextIndex and m.matchIndex
	// after loop: m.nextIndex == m.matchIndex + 1
	//
	// NOTE:
	//   we are not sending req.leaderCommitIndex, because
	//   we do not want follower to move its commit index, while we are
	//   trying to figure out which entries to replicate
	for m.matchIndex+1 != m.nextIndex {
		storage.fillEntries(req, m.nextIndex, m.nextIndex-1) // zero entries
		resp, err := m.retryAppendEntries(req, stopCh)
		if err != nil {
			return
		}
		if resp.success {
			matchIndex = req.prevLogIndex
			m.setMatchIndex(matchIndex, matchUpdatedCh)
			break
		} else {
			m.nextIndex--
		}
		select {
		case <-stopCh:
			return
		default:
		}
	}

	closedCh := func() <-chan time.Time {
		ch := make(chan time.Time)
		close(ch)
		return ch
	}()
	timerCh := closedCh

	for {
		select {
		case <-stopCh:
			return
		case lastIndex = <-m.leaderLastIndexCh:
			debug(ldr, m.addr, "got lastIndex update", lastIndex)
			timerCh = closedCh
		case req.leaderCommitIndex = <-m.leaderCommitIndexCh:
			debug(ldr, m.addr, "got commitIndex update", req.leaderCommitIndex)
			timerCh = closedCh
		default:
			<-timerCh
		}

		// setup request
		if matchIndex < lastIndex {
			// replication of entries [m.nextIndex, lastIndex] is pending
			maxIndex := min(lastIndex, m.nextIndex+uint64(maxAppendEntries)-1)
			storage.fillEntries(req, m.nextIndex, maxIndex)
			debug(ldr, m.addr, "sending", len(req.entries), "entries")
		} else {
			// send heartbeat
			req.prevLogIndex, req.prevLogTerm, req.entries = lastIndex, req.term, nil // zero entries
			debug(ldr, "heartbeat ->", m.addr)
		}

		resp, err := m.retryAppendEntries(req, stopCh)
		if err != nil || !resp.success {
			// resp.success will be false, if the follower have transitioned to candidate and started election
			return
		}
		if len(req.entries) > 0 {
			last := req.entries[len(req.entries)-1]
			m.nextIndex = last.index + 1
			matchIndex = last.index
			m.setMatchIndex(matchIndex, matchUpdatedCh)
		}

		if matchIndex < lastIndex {
			// replication of entries [m.nextIndex, lastIndex] is still pending: no more sleeping!!!
			timerCh = closedCh
		} else {
			timerCh = afterRandomTimeout(m.heartbeatTimeout / 10)
		}
	}
}

func (m *member) getMatchIndex() uint64 {
	return atomic.LoadUint64(&m.matchIndex)
}

func (m *member) setMatchIndex(v uint64, updatedCh chan<- *member) {
	atomic.StoreUint64(&m.matchIndex, v)
	updatedCh <- m
}
