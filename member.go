package raft

import (
	"sync"
	"time"
)

type member struct {
	addr             string
	timeout          time.Duration
	heartbeatTimeout time.Duration

	clientsLock sync.Mutex
	clients     []*client
	maxClients  int
}

func (m *member) getClient() (*client, error) {
	m.clientsLock.Lock()
	defer m.clientsLock.Unlock()

	num := len(m.clients)
	if num == 0 {
		return dial(m.addr, m.timeout)
	}
	var client *client
	client, m.clients[num-1] = m.clients[num-1], nil
	m.clients = m.clients[:num-1]
	return client, nil
}

func (m *member) returnClient(client *client) {
	m.clientsLock.Lock()
	defer m.clientsLock.Unlock()

	if len(m.clients) < m.maxClients {
		m.clients = append(m.clients, client)
	} else {
		client.close()
	}
}

func (m *member) doRPC(typ rpcType, req, resp command) error {
	c, err := m.getClient()
	if err != nil {
		return err
	}
	if err = c.doRPC(typ, req, resp); err != nil {
		c.close()
		return err
	}
	m.returnClient(c)
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

func (m *member) sendHeartbeat(req *appendEntriesRequest) {

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
