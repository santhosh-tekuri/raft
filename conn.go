package raft

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type netConn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	timeout time.Duration
}

type dialFn func(network, address string, timeout time.Duration) (net.Conn, error)

func dial(dialFn dialFn, address string, timeout time.Duration) (*netConn, error) {
	conn, err := dialFn("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	return &netConn{
		conn:    conn,
		r:       bufio.NewReader(conn),
		w:       bufio.NewWriter(conn),
		timeout: timeout,
	}, nil
}

func (n *netConn) doRPC(req request, resp message) error {
	if err := n.conn.SetDeadline(time.Now().Add(n.timeout)); err != nil {
		return err
	}
	if err := writeUint8(n.w, uint8(req.rpcType())); err != nil {
		return err
	}
	if err := req.encode(n.w); err != nil {
		return err
	}
	if err := n.w.Flush(); err != nil {
		return err
	}
	if err := resp.decode(n.r); err != nil {
		return err
	}
	return nil
}

func (n *netConn) close() error {
	return n.conn.Close()
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
	id       uint64
	resolver *resolver
	dialFn   dialFn
	timeout  time.Duration
	max      int

	mu    sync.Mutex
	conns []*netConn
}

func (pool *connPool) getConn() (*netConn, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	num := len(pool.conns)
	if num == 0 {
		addr, err := pool.resolver.lookupID(pool.id)
		if err != nil {
			return nil, err
		}
		return dial(pool.dialFn, addr, pool.timeout)
	}
	var conn *netConn
	conn, pool.conns[num-1] = pool.conns[num-1], nil
	pool.conns = pool.conns[:num-1]
	return conn, nil
}

func (pool *connPool) returnConn(conn *netConn) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if len(pool.conns) < pool.max {
		pool.conns = append(pool.conns, conn)
	} else {
		_ = conn.close()
	}
}

func (pool *connPool) closeAll() {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for _, conn := range pool.conns {
		_ = conn.close()
	}
	pool.conns = nil
}

// -----------------------------------------------------

func (r *Raft) getConnPool(id uint64) *connPool {
	pool, ok := r.connPools[id]
	if !ok {
		pool = &connPool{
			id:       id,
			resolver: r.resolver,
			dialFn:   r.dialFn,
			timeout:  10 * time.Second, // todo
			max:      3,                //todo
		}
		r.connPools[id] = pool
	}
	return pool
}
