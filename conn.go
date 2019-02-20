package raft

import (
	"bufio"
	"net"
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

func (n *netConn) doRPC(typ rpcType, req, resp command) error {
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
