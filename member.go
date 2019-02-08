package raft

import (
	"sync"
	"time"
)

type member struct {
	addr             string
	timeout          time.Duration
	heartbeatTimeout time.Duration

	connPoolMu sync.Mutex
	connPool   []*netConn
	maxConns   int
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

func (m *member) sendHeartbeats(heartbeat *appendEntriesRequest, stopCh <-chan struct{}) {
	var failures uint64
	send := func(req *appendEntriesRequest) (*appendEntriesResponse, error) {
		debug("sending appentries", m.addr, req)
		resp, err := m.appendEntries(req)
		debug("done", m.addr)
		if err != nil {
			failures++
			select {
			case <-time.After(backoff(failures)):
			case <-stopCh:
			}
		} else {
			failures = 0
		}
		return resp, err
	}

	send(heartbeat)
	for {
		select {
		case <-stopCh:
			return
		case <-afterRandomTimeout(m.heartbeatTimeout / 10):
		}
		send(heartbeat)
	}
}
