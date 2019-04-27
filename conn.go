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
	"net"
	"sync"
	"time"
)

type conn struct {
	rwc  net.Conn
	bufr *bufio.Reader
	bufw *bufio.Writer
}

type dialFn func(network, address string, timeout time.Duration) (net.Conn, error)

func dial(dialFn dialFn, address string, timeout time.Duration) (*conn, error) {
	rwc, err := dialFn("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	return &conn{
		rwc:  rwc,
		bufr: bufio.NewReader(rwc),
		bufw: bufio.NewWriter(rwc),
	}, nil
}

func (c *conn) writeReq(req request, deadline time.Time) error {
	if err := c.rwc.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := writeUint8(c.bufw, uint8(req.rpcType())); err != nil {
		return err
	}
	if err := req.encode(c.bufw); err != nil {
		return err
	}
	return c.bufw.Flush()
}

func (c *conn) readResp(resp response, deadline time.Time) error {
	if err := c.rwc.SetReadDeadline(deadline); err != nil {
		return err
	}
	return resp.decode(c.bufr)
}

func (c *conn) doRPC(req request, resp response, deadline time.Time) error {
	if err := c.writeReq(req, deadline); err != nil {
		return err
	}
	return c.readResp(resp, deadline)
}

// --------------------------------------------------------------------

type resolver struct {
	delegate Resolver // user given resolver
	logger   Logger
	alerts   Alerts
	mu       sync.RWMutex
	addrs    map[uint64]string
}

func (r *resolver) update(config Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range config.Nodes {
		r.addrs[n.ID] = n.Addr
	}
}

func (r *resolver) lookupID(id uint64, timeout time.Duration) string {
	if r.delegate != nil {
		addr, err := r.delegate.LookupID(id, timeout)
		if err == nil {
			return addr
		}
		err = opError(err, "Resolver.LookupID(%d)", id)
		r.logger.Warn(trimPrefix(err))
		r.alerts.Error(err)
	}

	// fallback
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.addrs[id]
}

// --------------------------------------------------------------------

type connPool struct {
	src      uint64
	cid      uint64
	nid      uint64
	resolver *resolver
	dialFn   dialFn
	max      int

	mu    sync.Mutex
	conns []*conn
}

func (pool *connPool) getConn(deadline time.Time) (*conn, error) {
	assert(!deadline.IsZero())
	var c *conn
	pool.mu.Lock()
	if num := len(pool.conns); num > 0 {
		c, pool.conns[num-1] = pool.conns[num-1], nil
		pool.conns = pool.conns[:num-1]
	}
	pool.mu.Unlock()
	if c != nil {
		return c, nil
	}

	// dial ---------
	addr := pool.resolver.lookupID(pool.nid, deadline.Sub(time.Now()))
	c, err := dial(pool.dialFn, addr, deadline.Sub(time.Now()))
	if err != nil {
		return nil, err
	}

	// check identity ---------
	resp := &identityResp{}
	err = c.doRPC(&identityReq{req: req{src: pool.src}, cid: pool.cid, nid: pool.nid}, resp, deadline)
	if err != nil || resp.result != success {
		_ = c.rwc.Close()
		return nil, IdentityError{pool.cid, pool.nid, addr}
	}
	return c, nil
}

func (pool *connPool) returnConn(c *conn) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if len(pool.conns) < pool.max {
		pool.conns = append(pool.conns, c)
	} else {
		_ = c.rwc.Close()
	}
}

func (pool *connPool) closeAll() {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for _, c := range pool.conns {
		_ = c.rwc.Close()
	}
	pool.conns = nil
}

func (pool *connPool) doRPC(req request, resp response, deadline time.Time) error {
	c, err := pool.getConn(deadline)
	if err != nil {
		return err
	}
	if err = c.doRPC(req, resp, deadline); err != nil {
		_ = c.rwc.Close()
		return err
	}
	pool.returnConn(c)
	return nil
}

type rpcResponse struct {
	response
	from uint64
	err  error
}

// -----------------------------------------------------

func (r *Raft) getConnPool(nid uint64) *connPool {
	pool, ok := r.connPools[nid]
	if !ok {
		pool = &connPool{
			src:      r.nid,
			cid:      r.cid,
			nid:      nid,
			resolver: r.resolver,
			dialFn:   r.dialFn,
			max:      1,
		}
		r.connPools[nid] = pool
	}
	return pool
}
