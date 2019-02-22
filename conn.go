package raft

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type netConn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	timeout time.Duration
}

type dialFn func(network, address string, timeout time.Duration) (net.Conn, error)

func dial(dialFn dialFn, target string, timeout time.Duration) (*netConn, error) {
	conn, err := dialFn("tcp", target, timeout)
	if err != nil {
		return nil, err
	}
	return &netConn{
		conn:    conn,
		r:       bufio.NewReader(conn),
		w:       bufio.NewWriter(conn),
		timeout: timeout,
	}, nil
}

func (n *netConn) doRPC(typ rpcType, req, resp message) error {
	if err := n.conn.SetDeadline(time.Now().Add(n.timeout)); err != nil {
		return err
	}
	if err := writeUint8(n.w, uint8(typ)); err != nil {
		return err
	}
	if err := req.encode(n.w); err != nil {
		return err
	}
	if err := n.w.Flush(); err != nil {
		return err
	}
	if err := resp.decode(n.r); err != nil {
		return err
	}
	return nil
}

func (n *netConn) close() error {
	return n.conn.Close()
}

// --------------------------------------------------------------------

type connPool struct {
	addr    string
	dialFn  dialFn
	timeout time.Duration
	max     int

	mu    sync.Mutex
	conns []*netConn
}

func (pool *connPool) getConn() (*netConn, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	num := len(pool.conns)
	if num == 0 {
		return dial(pool.dialFn, pool.addr, pool.timeout)
	}
	var conn *netConn
	conn, pool.conns[num-1] = pool.conns[num-1], nil
	pool.conns = pool.conns[:num-1]
	return conn, nil
}

func (pool *connPool) returnConn(conn *netConn) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if len(pool.conns) < pool.max {
		pool.conns = append(pool.conns, conn)
	} else {
		_ = conn.close()
	}
}
