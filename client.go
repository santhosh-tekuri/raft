package raft

import (
	"bufio"
	"net"
	"time"
)

type client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	timeout time.Duration
}

func dial(target string, timeout time.Duration) (*client, error) {
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return nil, err
	}
	return &client{
		conn:    conn,
		r:       bufio.NewReader(conn),
		w:       bufio.NewWriter(conn),
		timeout: timeout,
	}, nil
}

func (c *client) doRPC(typ rpcType, req, resp command) error {
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	if err := writeUint8(c.w, uint8(typ)); err != nil {
		return err
	}
	if err := req.encode(c.w); err != nil {
		return err
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	if err := resp.decode(c.r); err != nil {
		return err
	}
	return nil
}

func (c *client) close() error {
	return c.conn.Close()
}
