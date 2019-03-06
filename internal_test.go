package raft

// export access to raft internals for tests

func Debug(args ...interface{}) {
	debug(args...)
}

func RequestVote(from, to *Raft) (granted bool, err error) {
	t := Inspect(func(info Info) {
		req := &voteReq{
			term:         info.Term(),
			lastLogIndex: info.LastLogIndex(),
			lastLogTerm:  info.LastLogTerm(),
			candidate:    info.ID(),
		}
		connPool := from.getConnPool(to.id)
		cand := candShip{Raft: from}
		resp, errr := cand.requestVote(connPool, req)
		granted, err = resp.granted, errr
	})
	from.Tasks() <- t
	<-t.Done()
	return
}

func BootstrapStorage(storage Storage, nodes map[ID]Node) error {
	store := newStorage(storage)
	if err := store.init(); err != nil {
		return err
	}
	_, err := store.bootstrap(nodes)
	return err
}
