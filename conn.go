package raft

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type conn struct {
	rwc     net.Conn
	bufr    *bufio.Reader
	bufw    *bufio.Writer
	timeout time.Duration
}

type dialFn func(network, address string, timeout time.Duration) (net.Conn, error)

func dial(dialFn dialFn, address string, timeout time.Duration) (*conn, error) {
	rwc, err := dialFn("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	return &conn{
		rwc:     rwc,
		bufr:    bufio.NewReader(rwc),
		bufw:    bufio.NewWriter(rwc),
		timeout: timeout,
	}, nil
}

func (c *conn) doRPC(req request, resp message) error {
	if err := c.rwc.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return err
	}
	if err := writeUint8(c.bufw, uint8(req.rpcType())); err != nil {
		return err
	}
	if err := req.encode(c.bufw); err != nil {
		return err
	}
	if err := c.bufw.Flush(); err != nil {
		return err
	}
	return resp.decode(c.bufr)
}

// --------------------------------------------------------------------

type resolver struct {
	delegate Resolver // user given resolver
	trace    *Trace   // used to trace lookup failures
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

func (r *resolver) lookupID(id uint64) (string, error) {
	var addr string
	var err error

	if r.delegate != nil {
		addr, err = r.delegate.LookupID(id)
		if err == nil {
			return addr, nil
		}
		if r.trace.Error != nil {
			r.trace.Error(opError(err, "Resolver.LookupID(%q)", id))
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	addr = r.addrs[id]
	return addr, nil
}

// --------------------------------------------------------------------

type connPool struct {
	cid      uint64
	nid      uint64
	resolver *resolver
	dialFn   dialFn
	timeout  time.Duration
	max      int

	mu    sync.Mutex
	conns []*conn
}

func (pool *connPool) getConn() (*conn, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	num := len(pool.conns)
	if num == 0 {
		addr, err := pool.resolver.lookupID(pool.nid)
		if err != nil {
			return nil, err
		}
		c, err := dial(pool.dialFn, addr, pool.timeout)
		if err != nil {
			return nil, err
		}

		// check identity
		resp := &identityResp{}
		err = c.doRPC(&identityReq{cid: pool.cid, nid: pool.nid}, resp)
		if err != nil || resp.result != success {
			_ = c.rwc.Close()
			return nil, IdentityError{pool.cid, pool.nid, addr}
		}

		return c, nil
	}
	var c *conn
	c, pool.conns[num-1] = pool.conns[num-1], nil
	pool.conns = pool.conns[:num-1]
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

type rpcResponse struct {
	resp response
	from uint64
	err  error
}

func (pool *connPool) doRPC(req request, resp response, deadline time.Time, ch chan<- rpcResponse) {
	var err error
	defer func() {
		ch <- rpcResponse{resp, pool.nid, err}
	}()
	c, err := pool.getConn()
	if err != nil {
		return
	}
	if err = c.rwc.SetDeadline(deadline); err != nil {
		_ = c.rwc.Close()
		return
	}
	if err = c.doRPC(req, resp); err != nil {
		_ = c.rwc.Close()
		return
	}
	pool.returnConn(c)
}

// -----------------------------------------------------

func (r *Raft) getConnPool(nid uint64) *connPool {
	pool, ok := r.connPools[nid]
	if !ok {
		pool = &connPool{
			cid:      r.cid,
			nid:      nid,
			resolver: r.resolver,
			dialFn:   r.dialFn,
			timeout:  10 * time.Second, // todo
			max:      3,                //todo
		}
		r.connPools[nid] = pool
	}
	return pool
}
