package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// ErrServerClosed is returned by the Raft's Serve and ListenAndServe
// methods after a call to Shutdown
var ErrServerClosed = errors.New("raft: Server closed")

type rpc struct {
	req    command
	respCh chan<- command
}

type server struct {
	listenFn func(network, address string) (net.Listener, error)
	listener net.Listener
	rpcCh    chan rpc

	// to handle safe shutdown
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

func (s *server) listen(address string) error {
	listener, err := s.listenFn("tcp", address)
	if err != nil {
		return err
	}
	*s = server{
		listener:   listener,
		rpcCh:      make(chan rpc),
		shutdownCh: make(chan struct{}),
	}
	s.wg.Add(1) // The first increment must be synchronized with Wait
	return nil
}

func (s *server) serve() error {
	defer s.wg.Done()
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
		}
		err := s.handleRPC(conn, r, w)
		if err != nil {
			if err != io.EOF && err != ErrServerClosed {
				log.Printf("unexpected error from handleRPC: %v", err)
			}
			return
		}
	}
}

// if shutdown signal received, returns ErrServerClosed immediately
func (s *server) handleRPC(conn net.Conn, r *bufio.Reader, w *bufio.Writer) error {
	var typ rpcType
	// close client if idle, on shutdown signal
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
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
		conn.SetReadDeadline(time.Time{}) // clears deadline
		break
	}

	respCh := make(chan command, 1)
	rpc := rpc{respCh: respCh}

	// decode request
	switch typ {
	case rpcVote:
		req := &voteRequest{}
		rpc.req = req
	case rpcAppendEntries:
		req := &appendEntriesRequest{}
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

	// wait for response and send reply
	select {
	case <-s.shutdownCh:
		return ErrServerClosed
	case resp := <-respCh:
		// todo: set write deadline
		if err := resp.encode(w); err != nil {
			return err
		}
	}

	return w.Flush()
}

func (s *server) shutdown() {
	close(s.shutdownCh)
	_ = s.listener.Close()
	s.wg.Wait()
	close(s.rpcCh)
}
