package raft

// export access to raft internals for tests

func Debug(args ...interface{}) {
	debug(args)
}

func RequestVote(from, to *Raft) (granted bool, err error) {
	toAddr := to.Info().Addr()
	t := Inspect(func(info Info) {
		req := &voteRequest{
			term:         info.Term(),
			lastLogIndex: info.LastLogIndex(),
			lastLogTerm:  info.LastLogTerm(),
			candidate:    info.ID(),
		}
		connPool := from.getConnPool(toAddr)
		resp, errr := from.requestVote(connPool, req)
		granted, err = resp.granted, errr
	})
	from.Tasks() <- t
	<-t.Done()
	return
}
