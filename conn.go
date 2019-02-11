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

func dial(transport transport, target string, timeout time.Duration) (*netConn, error) {
	conn, err := transport.DialTimeout(target, timeout)
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
	n.conn.SetDeadline(time.Now().Add(n.timeout))
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
