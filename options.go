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
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

// todo: add roundThreshold & promoteThreshold, minRoundDuration

// Options contains necessary configuration for raft server.
//
// It is recommended that all servers in cluster use same options.
type Options struct {
	HeartbeatTimeout time.Duration
	QuorumWait       time.Duration

	// PromoteThreshold determines the minimum round duration required
	// for promoting a nonvoter.
	PromoteThreshold time.Duration

	// SnapshotInterval determines how often snapshot is taken.
	// The actual interval is staggered between this value and 2x of this value,
	// to avoid entire cluster from performing snapshot at same time.
	//
	// Zero value means don't take any snapshots automatically.
	SnapshotInterval time.Duration

	// SnapshotThreshold determines minimum number of log entries since recent
	// snapshot, in order to take snapshot.
	//
	// This is to avoid taking snapshot, for just few additional entries.
	SnapshotThreshold uint64

	// If ShutdownOnRemove is true, server will shutdown
	// when it is removed from the cluster.
	ShutdownOnRemove bool

	// Bandwidth is the network bandwidth in number of bytes per second.
	// This is used to compute I/O deadlines for AppendEntriesRequest
	// and InstallSnapshotRequest RPCs
	Bandwidth int64

	Trace    Trace
	Resolver Resolver
}

func (o Options) validate() error {
	if o.HeartbeatTimeout <= 0 {
		return errors.New("raft.options: invalid HeartbeatTimeout")
	}
	if o.PromoteThreshold <= 0 {
		return errors.New("raft.options: PromoteThreshold")
	}
	if o.Bandwidth <= 0 {
		return errors.New("raft.options: PromoteThreshold is zero")
	}
	return nil
}

// DefaultOptions returns an Options with usable defaults.
func DefaultOptions() Options {
	var mu sync.Mutex
	logger := func(prefix string) func(v ...interface{}) {
		return func(v ...interface{}) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Println(append(append([]interface{}(nil), prefix), v...)...)
		}
	}
	hbTimeout := 1000 * time.Millisecond
	return Options{
		HeartbeatTimeout: hbTimeout,
		QuorumWait:       0,
		PromoteThreshold: hbTimeout,
		SnapshotInterval: 2 * time.Hour,
		ShutdownOnRemove: true,
		Bandwidth:        256 * 1024,
		Trace:            DefaultTrace(logger("[INFO]"), logger("[WARN]")),
	}
}

// trace ----------------------------------------------------------

type Trace struct {
	Error               func(err error)
	Starting            func(info Info, addr net.Addr)
	StateChanged        func(info Info)
	LeaderChanged       func(info Info)
	ElectionStarted     func(info Info)
	ElectionAborted     func(info Info, reason string)
	CommitReady         func(info Info)
	ConfigChanged       func(info Info)
	ConfigCommitted     func(info Info)
	ConfigReverted      func(info Info)
	RoundCompleted      func(info Info, id uint64, round Round)
	LogCompacted        func(info Info)
	ConfigActionStarted func(info Info, id uint64, action ConfigAction)
	Unreachable         func(info Info, id uint64, since time.Time, err error)
	QuorumUnreachable   func(info Info, since time.Time)
	ShuttingDown        func(info Info, reason error)
}

func DefaultTrace(info, warn func(v ...interface{})) (trace Trace) {
	trace.Error = func(err error) {
		warn(err)
	}
	trace.Starting = func(rinfo Info, addr net.Addr) {
		info("raft: cid:", rinfo.CID(), "nid:", rinfo.NID())
		info("raft:", rinfo.Configs().Latest)
		info("raft: listening at", addr)
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
			info("raft: following leader node", rinfo.Leader())
		}
	}
	trace.ElectionStarted = func(rinfo Info) {
		info("raft: started election")
	}
	trace.ElectionAborted = func(rinfo Info, reason string) {
		info("raft: aborting election:", reason)
	}
	trace.CommitReady = func(rinfo Info) {
		info("raft: ready for commit")
	}
	trace.ConfigChanged = func(rinfo Info) {
		if rinfo.Configs().Latest.Index == 1 {
			info("raft: bootstrapped with", rinfo.Configs().Latest)
		} else {
			info("raft: config changed to", rinfo.Configs().Latest)
		}
	}
	trace.ConfigCommitted = func(rinfo Info) {
		info("raft: committed config", rinfo.Configs().Latest)
		if rinfo.Configs().IsStable() {
			info("raft: config is stable")
		}
	}
	trace.ConfigReverted = func(rinfo Info) {
		info("raft: reverted to", rinfo.Configs().Latest)
	}
	trace.RoundCompleted = func(rinfo Info, id uint64, r Round) {
		info("raft: nonVoter", id, "completed round", r.Ordinal, "in", r.Duration(), ", its lastIndex:", r.LastIndex)
	}
	trace.LogCompacted = func(rinfo Info) {
		info("raft: log upto index ", rinfo.FirstLogIndex()-1, "is discarded")
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
			warn("raft: node", id, "is unreachable since", since.Format(time.RFC3339), "reason:", err)
		}
	}
	trace.QuorumUnreachable = func(rinfo Info, since time.Time) {
		if since.IsZero() {
			info("raft: quorum is reachable now")
		} else {
			warn("raft: quorum is unreachable since", since.Format(time.RFC3339))
		}
	}
	trace.ShuttingDown = func(rinfo Info, reason error) {
		info("raft: shutting down, reason", strconv.Quote(reason.Error()))
	}
	return
}

// ----------------------------------------------

type Resolver interface {
	LookupID(id uint64, timeout time.Duration) (addr string, err error)
}
