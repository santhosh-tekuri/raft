package raft

// export access to raft internals for tests

func Debug(args ...interface{}) {
	debug(args)
}

func RequestVote(from, to *Raft) (granted bool, err error) {
	t := Inspect(func(info Info) {
		req := &voteRequest{
			term:         info.Term(),
			lastLogIndex: info.LastLogIndex(),
			lastLogTerm:  info.LastLogTerm(),
			candidate:    info.ID(),
		}
		connPool := from.getConnPool(to.id)
		resp, errr := from.requestVote(connPool, req)
		granted, err = resp.granted, errr
	})
	from.Tasks() <- t
	<-t.Done()
	return
}
