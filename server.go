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
	"net"
	"sync"
	"time"
)

type rpc struct {
	req     request
	resp    response
	conn    *conn
	readErr error // error while reading partial req payload
	done    chan struct{}
}

type server struct {
	lr           net.Listener
	stopCh       chan struct{}
	rpcCh        chan *rpc
	disconnected chan uint64 // nid
}

func newServer(lr net.Listener) *server {
	return &server{
		lr:     lr,
		stopCh: make(chan struct{}),
		rpcCh:  make(chan *rpc),
	}
}

func (s *server) serve() {
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
			_ = s.handleConn(conn)
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
	close(s.rpcCh)
}

func (s *server) handleConn(rwc net.Conn) error {
	c := &conn{
		rwc:  rwc,
		bufr: bufio.NewReader(rwc),
		bufw: bufio.NewWriter(rwc),
	}

	var nid uint64
	defer func() {
		if nid != 0 {
			select {
			case <-s.stopCh:
			case s.disconnected <- nid:
			}
		}
	}()
	for !isClosed(s.stopCh) {
		// clear deadline
		if err := c.rwc.SetReadDeadline(time.Time{}); err != nil {
			return err
		}
		b, err := c.bufr.ReadByte()
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
		rpc := &rpc{req: rpcType(b).createReq(), conn: c, done: make(chan struct{})}

		// decode request
		// todo: set read deadline
		if err := rpc.req.decode(c.bufr); err != nil {
			return err
		}

		// send request for processing
		select {
		case <-s.stopCh:
			return ErrServerClosed
		case s.rpcCh <- rpc:
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
		if rpc.req.rpcType() == rpcIdentity && rpc.resp.getResult() == success {
			nid = rpc.req.from()
		}
		// todo: set write deadline
		if err = rpc.resp.encode(c.bufw); err != nil {
			return err
		}
		if err = c.bufw.Flush(); err != nil {
			return err
		}
		if rpc.req.rpcType() == rpcIdentity && rpc.resp.getResult() != success {
			return IdentityError{}
		}
	}
	return nil
}

func (s *server) shutdown() {
	close(s.stopCh)
	_ = s.lr.Close()
}
