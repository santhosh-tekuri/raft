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
	mu         sync.RWMutex
	lr         net.Listener
	rpcCh      chan *rpc
	wg         sync.WaitGroup
	shutdownCh chan struct{}
}

func newServer() *server {
	return &server{
		rpcCh:      make(chan *rpc),
		shutdownCh: make(chan struct{}),
	}
}

// todo: note that we dont support multiple listeners
func (s *server) serve(l net.Listener) {
	defer s.wg.Done()

	s.mu.Lock()
	if s.isClosed() {
		_ = l.Close()
	}
	s.wg.Add(1) // The first increment must be synchronized with Wait
	s.lr = l
	s.mu.Unlock()

	conns := make(map[net.Conn]struct{})
	var wg sync.WaitGroup
	for !s.isClosed() {
		conn, err := s.lr.Accept()
		if err != nil {
			continue
		}
		s.mu.Lock()
		conns[conn] = struct{}{}
		s.mu.Unlock()
		wg.Add(1)
		go func() {
			r, w := bufio.NewReader(conn), bufio.NewWriter(conn)
			for !s.isClosed() {
				if err := s.handleRPC(conn, r, w); err != nil {
					break
				}
			}
			s.mu.Lock()
			delete(conns, conn)
			s.mu.Unlock()
			_ = conn.Close()
			wg.Done()
		}()
	}

	s.mu.RLock()
	for conn := range conns {
		_ = conn.Close()
	}
	s.mu.RUnlock()
	wg.Wait()
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
	if s.lr != nil {
		_ = s.lr.Close()
	}
	s.mu.RUnlock()
	s.wg.Wait()
	close(s.rpcCh)
}
