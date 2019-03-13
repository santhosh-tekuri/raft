package raft

import (
	"bufio"
	"io"
	"net"
	"sync"
)

type rpc struct {
	req     request
	reader  io.Reader // for partial requests
	resp    message
	readErr error // error while reading partial req payload
	done    chan struct{}
}

type server struct {
	mu       sync.RWMutex
	listener net.Listener
	conns    map[net.Conn]struct{}

	rpcCh      chan *rpc
	wg         sync.WaitGroup
	shutdownCh chan struct{}
}

func newServer() *server {
	return &server{
		rpcCh:      make(chan *rpc),
		shutdownCh: make(chan struct{}),
		conns:      make(map[net.Conn]struct{}),
	}
}

// todo: note that we dont support multiple listeners
func (s *server) serve(l net.Listener) {
	defer func() {
		s.mu.RLock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.RUnlock()
		s.wg.Done()
	}()

	s.mu.Lock()
	if s.isClosed() {
		_ = l.Close()
	}
	s.wg.Add(1) // The first increment must be synchronized with Wait
	s.listener = l
	s.mu.Unlock()

	for !s.isClosed() {
		conn, err := s.listener.Accept()
		if err != nil {
			continue
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.handleClient(conn)
	}
}

func (s *server) handleClient(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
		s.wg.Done()
	}()
	r, w := bufio.NewReader(conn), bufio.NewWriter(conn)
	for !s.isClosed() {
		if err := s.handleRPC(conn, r, w); err != nil {
			return
		}
	}
}

// if shutdown signal received, returns ErrServerClosed immediately
func (s *server) handleRPC(conn net.Conn, r *bufio.Reader, w *bufio.Writer) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	rpc := &rpc{req: rpcType(b).createReq(), done: make(chan struct{}), reader: r}

	// decode request
	// todo: set read deadline
	if err := rpc.req.decode(r); err != nil {
		return err
	}

	// send request for processing
	select {
	case <-s.shutdownCh:
		return ErrServerClosed
	case s.rpcCh <- rpc:
	}

	// wait for response
	select {
	case <-s.shutdownCh:
		return ErrServerClosed
	case <-rpc.done:
	}

	// send reply
	if rpc.readErr != nil {
		return rpc.readErr
	}
	// todo: set write deadline
	if err = rpc.resp.encode(w); err != nil {
		return err
	}
	return w.Flush()
}

func (s *server) isClosed() bool {
	select {
	case <-s.shutdownCh:
		return true
	default:
		return false
	}
}
func (s *server) shutdown() {
	s.mu.RLock()
	close(s.shutdownCh)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.mu.RUnlock()
	s.wg.Wait()
	close(s.rpcCh)
}
