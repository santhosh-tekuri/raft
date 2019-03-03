package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ErrServerClosed is returned by the Raft's Serve and ListenAndServe
// methods after a call to Shutdown
var ErrServerClosed = errors.New("raft: Server closed")

type rpc struct {
	req     message
	reader  io.Reader // for partial requests
	resp    message
	readErr error // error while reading partial req payload
	done    chan struct{}
}

type server struct {
	listenerMu sync.RWMutex
	listener   net.Listener
	rpcCh      chan *rpc

	// interval to check for shutdown signal
	idleTimeout time.Duration

	// to handle safe shutdown
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

func newServer(idleTimeout time.Duration) *server {
	return &server{
		rpcCh:       make(chan *rpc),
		shutdownCh:  make(chan struct{}),
		idleTimeout: idleTimeout,
	}
}

// todo: note that we dont support multiple listeners
func (s *server) serve(l net.Listener) error {
	s.wg.Add(1) // The first increment must be synchronized with Wait
	defer s.wg.Done()

	s.listenerMu.Lock()
	select {
	case <-s.shutdownCh:
		_ = l.Close()
	default:
	}
	s.listener = l
	s.listenerMu.Unlock()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownCh:
				return ErrServerClosed
			default:
				continue
			}
		}
		s.wg.Add(1)
		go s.handleClient(conn)
	}
}

func (s *server) handleClient(conn net.Conn) {
	defer conn.Close()
	defer s.wg.Done()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		select {
		case <-s.shutdownCh:
			return
		default:
			if err := s.handleRPC(conn, r, w); err != nil {
				return
			}
		}
	}
}

// if shutdown signal received, returns ErrServerClosed immediately
func (s *server) handleRPC(conn net.Conn, r *bufio.Reader, w *bufio.Writer) error {
	var typ rpcType
	// close client if idle, on shutdown signal
	for {
		// todo: use setting past deadline technique for this
		if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			return err
		}
		b, err := r.ReadByte()
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				select {
				case <-s.shutdownCh:
					return ErrServerClosed
				default:
					continue
				}
			}
			return err
		}
		typ = rpcType(b)
		if err := conn.SetReadDeadline(time.Time{}); err != nil { // clears deadline
			return err
		}
		break
	}

	rpc := &rpc{done: make(chan struct{}), reader: r}

	// decode request
	switch typ {
	case rpcVote:
		req := &voteReq{}
		rpc.req = req
	case rpcAppendEntries:
		req := &appendEntriesReq{}
		rpc.req = req
	case rpcInstallSnap:
		req := &installSnapReq{}
		rpc.req = req
	default:
		return fmt.Errorf("unknown rpcType: %d", typ)
	}
	// todo: set read deadline
	if err := rpc.req.decode(r); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
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
	if err := rpc.resp.encode(w); err != nil {
		return err
	}
	return w.Flush()
}

func (s *server) shutdown() {
	close(s.shutdownCh)

	s.listenerMu.RLock()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.listenerMu.RUnlock()

	s.wg.Wait()
	close(s.rpcCh)
}
