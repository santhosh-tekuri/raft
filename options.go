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
	"sync"
	"time"
)

// todo: add roundThreshold & promoteThreshold, minRoundDuration

// Options contains necessary configuration for raft server.
//
// It is recommended that all servers in cluster use same options.
type Options struct {
	HeartbeatTimeout time.Duration

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

	// LogSegmentSize is the size of logSegmentFile in bytes. Raft log is
	// a collection of segment files. When current segment file is full,
	// new segment file is created. Value must be >=1024.
	LogSegmentSize int

	// SnapshotsRetain is the number of snapshots to be retained locally.
	// When new snapshot is taken, older snapshots are removed accordingly.
	// Value must be >=1.
	SnapshotsRetain int

	// Logger used for logging messages. If nil, nothing is logged.
	Logger Logger

	// Alerts used to consume alerts that are raised. If nil, no alerts
	// will be raised.
	Alerts Alerts

	// Resolver used to resolved node id to transport address. If nill,
	// Node.Address is used.
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
	if o.SnapshotsRetain < 1 {
		return errors.New("raft.options: must retain at least one snapshot")
	}
	if o.LogSegmentSize < 1024 {
		return fmt.Errorf("raft.options: LogSegmentSize is too smal")
	}
	return nil
}

// DefaultOptions returns an Options with usable defaults.
func DefaultOptions() Options {
	hbTimeout := 1000 * time.Millisecond
	return Options{
		HeartbeatTimeout:  hbTimeout,
		PromoteThreshold:  hbTimeout,
		SnapshotInterval:  2 * time.Hour,
		SnapshotThreshold: 8192,
		ShutdownOnRemove:  true,
		Bandwidth:         256 * 1024,
		LogSegmentSize:    16 * 1024 * 1024,
		SnapshotsRetain:   1,
		Logger:            new(defaultLogger),
	}
}

// Resolver used to resolve node id to transport address.
// Without resolver, config must be updated with new address.
// Resolves becomes handy, when raft is deployed in container or cloud
// environment, where the address can be resolved using tags.
type Resolver interface {
	// LookupID returns the transport address for given node id.
	// On error, the transport address specified in config is used.
	LookupID(id uint64, timeout time.Duration) (addr string, err error)
}

// Logger is the interface to be implemented for
// consuming logs.
type Logger interface {
	// Info consumes information message.
	Info(v ...interface{})

	// Warn consumes warning message.
	Warn(v ...interface{})
}

type nopLogger struct{}

func (nopLogger) Info(v ...interface{}) {}
func (nopLogger) Warn(v ...interface{}) {}

type defaultLogger struct {
	mu sync.Mutex
}

func (l *defaultLogger) Info(v ...interface{}) {
	l.mu.Lock()
	fmt.Print("[INFO] raft: ")
	fmt.Println(v...)
	l.mu.Unlock()
}

func (l *defaultLogger) Warn(v ...interface{}) {
	l.mu.Lock()
	fmt.Print("[WARN] raft: ")
	fmt.Println(v...)
	l.mu.Unlock()
}

// Alerts allows to consume any alerts raised by raft.
// This is useful in raising/resolving tickets to devops
// automatically.
type Alerts interface {
	// Error alerts that an error is occurred but raft is
	// able to continue to run. For example:
	// - error in taking snapshot
	// - error in log compaction
	// - error in Resolver.LookupID
	Error(err error)

	// UnReachable alert is raised by leader, when it finds that
	// a follower node is not unreachable. The err tells the reason
	// whey the node is treated as unreachable. A node is treated as
	// unreachable in following cases:
	// - error in dialing
	// - unexpected error in I/O, including timeout.
	// - node is reachable, but behaving un-appropriately, such as
	//   rejecting matchIndex. this happens if node is restarted with
	//   empty storage dir
	//
	// It is recommended to treat this as serious if "Reachable" alert
	// is not raised within some configurable time.
	UnReachable(id uint64, err error)

	// Reachable alert is raised by leader, when a node that was unreachable
	// is now reachable.
	Reachable(id uint64)

	// QuorumUnreachable alert is raised by leader, on detecting that quorum
	// is no longer available and it is stepping down to follower state.
	//
	// It is recommended to treat this as serious, if leader got election after
	// this alert within some configurable time.
	QuorumUnreachable()

	// ShuttingDown alert is raised when raft server is shutting down.
	//
	// If is recommended to treat this as serious if reason is something other
	// than ErrServerClosed and ErrNodeRemoved. For example, OpError signals
	// that there is some issue with storage or FSM.
	ShuttingDown(reason error)
}

type nopAlerts struct{}

func (nopAlerts) Error(err error)                  {}
func (nopAlerts) UnReachable(id uint64, err error) {}
func (nopAlerts) Reachable(id uint64)              {}
func (nopAlerts) QuorumUnreachable()               {}
func (nopAlerts) ShuttingDown(reason error)        {}

var tracer struct {
	error               func(err error)
	stateChanged        func(r *Raft)
	leaderChanged       func(r *Raft)
	electionStarted     func(r *Raft)
	electionAborted     func(r *Raft, reason string)
	commitReady         func(r *Raft)
	configChanged       func(r *Raft)
	configCommitted     func(r *Raft)
	configReverted      func(r *Raft)
	roundCompleted      func(r *Raft, id uint64, round round)
	logCompacted        func(r *Raft)
	configActionStarted func(r *Raft, id uint64, action Action)
	unreachable         func(r *Raft, id uint64, since time.Time, err error)
	quorumUnreachable   func(r *Raft, since time.Time)
	shuttingDown        func(r *Raft, reason error)
}
