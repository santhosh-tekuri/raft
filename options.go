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

	LogSegmentSize  int
	SnapshotsRetain int

	Logger   Logger
	Alerts   Alerts
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
		return errors.New("raft: must retain at least one snapshot")
	}
	if o.LogSegmentSize < 1024 {
		return fmt.Errorf("raft: LogSegmentSize is too smal")
	}
	return nil
}

// DefaultOptions returns an Options with usable defaults.
func DefaultOptions() Options {
	hbTimeout := 1000 * time.Millisecond
	return Options{
		HeartbeatTimeout: hbTimeout,
		QuorumWait:       0,
		PromoteThreshold: hbTimeout,
		SnapshotInterval: 2 * time.Hour,
		ShutdownOnRemove: true,
		Bandwidth:        256 * 1024,
		LogSegmentSize:   16 * 1024 * 1024,
		SnapshotsRetain:  1,
		Logger:           new(defaultLogger),
	}
}

type Resolver interface {
	LookupID(id uint64, timeout time.Duration) (addr string, err error)
}

type Logger interface {
	Info(v ...interface{})
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

type Alerts interface {
	Error(err error)
	UnReachable(id uint64, err error)
	Reachable(id uint64)
	QuorumUnreachable()
	ShuttingDown(reason error)
}

type nopAlerts struct{}

func (nopAlerts) Error(err error)                  {}
func (nopAlerts) UnReachable(id uint64, err error) {}
func (nopAlerts) Reachable(id uint64)              {}
func (nopAlerts) QuorumUnreachable()               {}
func (nopAlerts) ShuttingDown(reason error)        {}

type tracer struct {
	error               func(err error)
	stateChanged        func(r *Raft)
	leaderChanged       func(info Info)
	electionStarted     func(info Info)
	electionAborted     func(info Info, reason string)
	commitReady         func(info Info)
	configChanged       func(info Info)
	configCommitted     func(info Info)
	configReverted      func(info Info)
	roundCompleted      func(info Info, id uint64, round round)
	logCompacted        func(info Info)
	configActionStarted func(info Info, id uint64, action ConfigAction)
	unreachable         func(info Info, id uint64, since time.Time, err error)
	quorumUnreachable   func(info Info, since time.Time)
	shuttingDown        func(info Info, reason error)
}
