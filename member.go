package raft

import (
	"sync"
	"time"
)

type leaderUpdate struct {
	lastIndex, commitIndex uint64
}

type member struct {
	dialFn  dialFn
	addr    string
	timeout time.Duration

	connPoolMu sync.Mutex
	connPool   []*netConn
	maxConns   int

	// owned exclusively by raft main goroutine
	// used to recalculateMatch
	matchIndex uint64

	// from what time the replication unable to reach this member
	// zero value means it is reachable
	noContactMu sync.RWMutex
	noContact   time.Time
}

func (m *member) getConn() (*netConn, error) {
	m.connPoolMu.Lock()
	defer m.connPoolMu.Unlock()

	num := len(m.connPool)
	if num == 0 {
		return dial(m.dialFn, m.addr, m.timeout)
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
		_ = conn.close()
	}
}

func (m *member) doRPC(typ rpcType, req, resp message) error {
	conn, err := m.getConn()
	if err != nil {
		return err
	}
	if err = conn.doRPC(typ, req, resp); err != nil {
		_ = conn.close()
		return err
	}
	m.returnConn(conn)
	return nil
}

func (m *member) requestVote(req *voteRequest) (*voteResponse, error) {
	resp := new(voteResponse)
	err := m.doRPC(rpcVote, req, resp)
	return resp, err
}

func (m *member) contactSucceeded(b bool) {
	m.noContactMu.Lock()
	if b {
		m.noContact = time.Time{} // zeroing
	} else if m.noContact.IsZero() {
		m.noContact = time.Now()
	}
	m.noContactMu.Unlock()
}

// did we have success full contact after time t
func (m *member) contactedAfter(t time.Time) bool {
	m.noContactMu.RLock()
	noContact := m.noContact
	m.noContactMu.RUnlock()
	return noContact.IsZero() || noContact.After(t)
}
