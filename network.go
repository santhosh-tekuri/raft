package raft

import (
	"net"
	"time"
)

type transport interface {
	Listen(address string) (net.Listener, error)
	DialTimeout(address string, timeout time.Duration) (net.Conn, error)
}

type tcpTransport struct{}

func (t tcpTransport) Listen(address string) (net.Listener, error) {
	return net.Listen("tcp", address)
}

func (t tcpTransport) DialTimeout(address string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address, timeout)
}
