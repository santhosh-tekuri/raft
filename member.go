package raft

import (
	"log"
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

	nextIndex  uint64
	matchIndex uint64

	// leader notifies replicator when its lastLogIndex changes
	leaderLastIndexCh chan uint64
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

func (m *member) replicate(storage *storage, heartbeat *appendEntriesRequest, leaderCommitIndex uint64, stopCh <-chan struct{}) {
	// send initial empty AppendEntries RPCs (heartbeat) to each follower
	debug("heartbeat ->")
	m.retryAppendEntries(heartbeat, stopCh)

	req := &appendEntriesRequest{}
	*req = *heartbeat

	lastIndex := <-m.leaderLastIndexCh // non-blocking

	// know which entries to replicate: fixes m.nextIndex and m.matchIndex
	// after loop: m.nextIndex == m.matchIndex + 1
	for lastIndex >= m.nextIndex {
		storage.fillEntries(req, m.nextIndex, m.nextIndex-1) // zero entries
		resp, err := m.retryAppendEntries(req, stopCh)
		if err != nil {
			return
		}
		if resp.success {
			m.matchIndex = req.prevLogIndex
			break
		} else {
			m.nextIndex--
			continue
		}
	}

	for {
		// replicate entries [m.nextIndex, lastIndex] to follower
		for m.matchIndex < lastIndex {
			maxIndex := min(lastIndex, m.nextIndex+uint64(maxAppendEntries)-1)
			storage.fillEntries(req, m.nextIndex, maxIndex)
			debug("sending", len(req.entries), "entries to", m.addr)
			resp, err := m.retryAppendEntries(req, stopCh)
			if err != nil {
				return
			}
			if resp.success {
				m.nextIndex = maxIndex + 1
				m.matchIndex = maxIndex
			} else {
				log.Println("[WARN] should not happend") // todo
			}
		}

		// send heartbeat during idle periods to
		// prevent election timeouts
	loop:
		for {
			select {
			case <-stopCh:
				return
			case lastIndex = <-m.leaderLastIndexCh:
				break loop // to replicate new entry
			case <-afterRandomTimeout(m.heartbeatTimeout / 10):
				debug("heartbeat ->")
				m.retryAppendEntries(heartbeat, stopCh)
			}
		}
	}
}
