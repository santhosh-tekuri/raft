package raft

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type rpc struct {
	req      command
	respChan chan<- command
}

type server struct {
	listener net.Listener
	calls    chan rpc

	// to handle safe shutdown
	shutdownCh chan struct{}
	clients    sync.WaitGroup
}

func startServer(address string) (*server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &server{
		listener:   listener,
		calls:      make(chan rpc),
		shutdownCh: make(chan struct{}),
	}
	go s.serve()
	return s, nil
}

func (s *server) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownCh:
				return
			default:
				continue
			}
		}
		s.clients.Add(1)
		go s.handleClient(conn)
	}
}

func (s *server) handleClient(conn net.Conn) {
	defer conn.Close()
	defer s.clients.Done()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		err := s.handleRPC(conn, r, w)
		if err != nil {
			if err != io.EOF {
				log.Printf("error in handleRPC: %v", err)
			}
			return
		}
		select {
		case <-s.shutdownCh:
			return
		default:
			continue
		}
	}
}

func (s *server) handleRPC(conn net.Conn, r *bufio.Reader, w *bufio.Writer) error {
	var typ rpcType
	// reads rpcType of next rpc. handles shutdown signal
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		b, err := r.ReadByte()
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				select {
				case <-s.shutdownCh:
					return nil
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

	respChan := make(chan command, 1)
	call := rpc{respChan: respChan}

	switch typ {
	case rpcRequestVote:
		req := &requestVoteRequest{}
		call.req = req
	case rpcAppendEntries:
		req := &appendEntriesRequest{}
		call.req = req
	default:
		return fmt.Errorf("unknown rpcType: %d", typ)
	}

	if err := call.req.decode(r); err != nil {
		return err
	}
	s.calls <- call
	resp := <-respChan
	if err := resp.encode(w); err != nil {
		return err
	}
	return w.Flush()
}

func (s *server) shutdown() {
	close(s.shutdownCh)
	s.listener.Close()
	s.clients.Wait()
	close(s.calls)
}
