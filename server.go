// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
)

type rpc struct {
	req     request
	resp    response
	reader  io.Reader
	readErr error // error while reading partial req payload
	done    chan struct{}
}

type server struct {
	lr     net.Listener
	stopCh chan struct{}
}

func newServer(lr net.Listener) *server {
	return &server{
		lr:     lr,
		stopCh: make(chan struct{}),
	}
}

func (s *server) serve(rpcCh chan<- *rpc) {
	var wg sync.WaitGroup
	var mu sync.RWMutex
	conns := make(map[net.Conn]struct{})
	for !isClosed(s.stopCh) {
		conn, err := s.lr.Accept()
		if err != nil {
			continue
		}
		mu.Lock()
		conns[conn] = struct{}{}
		mu.Unlock()

		wg.Add(1)
		go func() {
			r, w := bufio.NewReader(conn), bufio.NewWriter(conn)
			for !isClosed(s.stopCh) {
				if err := s.handleRPC(rpcCh, r, w); err != nil {
					break
				}
			}
			mu.Lock()
			delete(conns, conn)
			mu.Unlock()
			_ = conn.Close()
			wg.Done()
		}()
	}

	mu.RLock()
	for conn := range conns {
		_ = conn.Close()
	}
	mu.RUnlock()
	wg.Wait()
	close(rpcCh)
}

// if shutdown signal received, returns ErrServerClosed immediately
func (s *server) handleRPC(ch chan<- *rpc, r *bufio.Reader, w *bufio.Writer) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	if !rpcType(b).isValid() {
		err = fmt.Errorf("raft: server.handleRpc got rpcType %d", b)
		if testMode {
			panic(err)
		}
		return err
	}
	rpc := &rpc{req: rpcType(b).createReq(), reader: r, done: make(chan struct{})}

	// decode request
	// todo: set read deadline
	if err := rpc.req.decode(r); err != nil {
		return err
	}

	// send request for processing
	select {
	case <-s.stopCh:
		return ErrServerClosed
	case ch <- rpc:
	}

	// wait for response
	select {
	case <-s.stopCh:
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

func (s *server) shutdown() {
	close(s.stopCh)
	_ = s.lr.Close()
}
