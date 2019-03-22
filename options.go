package raft

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// todo: add roundThreshold & promoteThreshold, minRoundDuration

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
	Error               func(err error)
	Starting            func(info Info)
	StateChanged        func(info Info)
	LeaderChanged       func(info Info)
	ElectionStarted     func(info Info)
	ElectionAborted     func(info Info, reason string)
	ConfigChanged       func(info Info)
	ConfigCommitted     func(info Info)
	ConfigReverted      func(info Info)
	RoundCompleted      func(info Info, id uint64, round Round)
	ConfigActionStarted func(info Info, id uint64, action ConfigAction)
	Unreachable         func(info Info, id uint64, since time.Time, err error)
	QuorumUnreachable   func(info Info, since time.Time)
	ShuttingDown        func(info Info, reason error)
}

func DefaultTrace(info, warn func(v ...interface{})) (trace Trace) {
	trace.Error = func(err error) {
		warn(err)
	}
	trace.Starting = func(rinfo Info) {
		info("raft: starting with cid:", rinfo.CID(), "nid:", rinfo.NID())
		info("raft: config", rinfo.Configs().Latest)
	}
	trace.StateChanged = func(rinfo Info) {
		info("raft: state changed to", rinfo.State())
	}
	trace.LeaderChanged = func(rinfo Info) {
		if rinfo.Leader() == 0 {
			info("raft: no known leader")
		} else if rinfo.Leader() == rinfo.NID() {
			info("raft: cluster leadership acquired")
		} else {
			info("raft: cluster leadership acquired by node", rinfo.Leader())
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
	trace.RoundCompleted = func(rinfo Info, id uint64, r Round) {
		info("raft: nonVoter", id, "completed round", r.Ordinal, "in", r.Duration(), ", its lastIndex:", r.LastIndex)
	}
	trace.ConfigActionStarted = func(rinfo Info, id uint64, action ConfigAction) {
		switch action {
		case Promote:
			info("raft: promoting nonvoter ", id, ", after", rinfo.Followers()[id].Round, "round(s)")
		case Demote:
			info("raft: demoting voter", id)
		case Remove:
			info("raft: removing nonvoter", id)
		}
	}
	trace.Unreachable = func(rinfo Info, id uint64, since time.Time, err error) {
		if since.IsZero() {
			info("raft: node", id, "is reachable now")
		} else {
			warn("raft: node", id, "is unreachable since", since, ":", err)
		}
	}
	trace.ShuttingDown = func(rinfo Info, reason error) {
		info("raft: shutting down, reason", strconv.Quote(reason.Error()))
	}
	return
}

// ----------------------------------------------

type Resolver interface {
	LookupID(id uint64) (addr string, err error)
}
