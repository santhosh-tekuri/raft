package raft

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Options struct {
	HeartbeatTimeout  time.Duration
	QuorumWait        time.Duration
	PromoteThreshold  time.Duration
	SnapshotThreshold uint64
	Trace             Trace
	Resolver          Resolver
}

func (o Options) validate() error {
	if o.HeartbeatTimeout == 0 {
		return errors.New("raft.options: HeartbeatTimeout is zero")
	}
	if o.PromoteThreshold == 0 {
		return errors.New("raft.options: PromoteThreshold is zero")
	}
	return nil
}

func DefaultOptions() Options {
	var mu sync.Mutex
	logger := func(prefix string) func(v ...interface{}) {
		return func(v ...interface{}) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Println(append(append([]interface{}(nil), prefix), v...))
		}
	}
	hbTimeout := 1000 * time.Millisecond
	return Options{
		HeartbeatTimeout: hbTimeout,
		QuorumWait:       hbTimeout,
		PromoteThreshold: hbTimeout,
		Trace:            DefaultTrace(logger("[INFO]"), logger("[WARN]")),
	}
}

// trace ----------------------------------------------------------

type Trace struct {
	Error           func(err error)
	Starting        func(info Info)
	StateChanged    func(info Info)
	ElectionStarted func(info Info)
	ElectionAborted func(info Info, reason string)
	ConfigChanged   func(info Info)
	ConfigCommitted func(info Info)
	ConfigReverted  func(info Info)
	RoundCompleted  func(info Info, id, round, lastIndex uint64, d time.Duration)
	Promoting       func(info Info, id, rounds uint64)
	Unreachable     func(info Info, id uint64, since time.Time) // todo: can we give err also
	ShuttingDown    func(info Info)

	sending  func(self, to uint64, state State, msg message)
	received func(self, from uint64, state State, term uint64, msg message)
}

func DefaultTrace(info, warn func(v ...interface{})) (trace Trace) {
	trace.Error = func(err error) {
		warn(err)
	}
	trace.Starting = func(rinfo Info) {
		info("raft: starting with Config", rinfo.Configs().Latest)
	}
	trace.StateChanged = func(rinfo Info) {
		if rinfo.State() == Leader {
			info("raft: cluster leadership acquired")
		}
	}
	trace.ElectionAborted = func(rinfo Info, reason string) {
		info("raft: aborting election:", reason)
	}
	trace.ConfigChanged = func(rinfo Info) {
		if rinfo.State() == Leader {
			info("raft: config changed to", rinfo.Configs().Latest)
		}
	}
	trace.ConfigCommitted = func(rinfo Info) {
		if rinfo.State() == Leader {
			info("raft: config committed")
		}
	}
	trace.RoundCompleted = func(rinfo Info, id, round, lastIndex uint64, d time.Duration) {
		info("raft: nonVoter", id, "completed round", round, "in", d, ", its lastIndex:", lastIndex)
	}
	trace.Promoting = func(rinfo Info, id, rounds uint64) {
		info("raft: promoting node", id, "to voter, after", rounds, "rounds")
	}
	trace.Unreachable = func(rinfo Info, id uint64, since time.Time) {
		if since.IsZero() {
			info("raft: node", id, "is reachable now")
		} else {
			warn("raft: node", id, "is unreachable since", since)
		}
	}
	trace.ShuttingDown = func(rinfo Info) {
		info("raft: shutting down")
	}
	return
}

// ----------------------------------------------

type Resolver interface {
	LookupID(id uint64) (addr string, err error)
}
