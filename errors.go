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
	"fmt"
	"runtime"
)

var (
	// ErrLockExists is returned by Raft.Serve and SetIdentity if the storageDir
	// already contains a lock file
	ErrLockExists = plainError("raft: lock file exists in storageDir")

	// ErrServerClosed is returned by the Raft.Serve if it was closed due to Shutdown call
	ErrServerClosed = plainError("raft: server closed")

	// ErrNodeRemoved is returned by the Raft.Serve if current node is removed from cluster
	ErrNodeRemoved = plainError("raft: node removed")

	// ErrIdentityAlreadySet is returned by Storage.SetIdentity, if you are trying
	// to override current identity.
	ErrIdentityAlreadySet = plainError("raft: identity already set")

	// ErrIdentityNotSet is returned by Raft.New, if no identity set in storage.
	ErrIdentityNotSet = plainError("raft: identity not set")

	// ErrFaultyFollower signals that the follower is faulty, and should be
	// removed from cluster. Such follower is treated as unreachable by leader.
	// This used use by Alerts.Unreachable and Replication.Err.
	ErrFaultyFollower = plainError("raft: faulty follower, denies matchIndex")

	// ErrNotCommitReady is returned by ChangeConfig, if leader is not yet ready to commit.
	// User can retry ChangeConfig after some time in case of this error.
	ErrNotCommitReady = temporaryError("raft.configChange: not ready to commit")

	// ErrStaleConfig indicates that the index of config submitted for change does not match
	// with latest config's index.
	ErrStaleConfig = plainError("raft.changeConfig: submitted config is stale")

	// ErrSnapshotThreshold indicates that TakeSnapshot task failed because there less than threshold edits
	// since last snapshot.
	ErrSnapshotThreshold = plainError("raft.takeSnapshot: not enough outstanding logs to snapshot")

	// ErrNoUpdates indicates that TakeSnapshot task failed because there are no edits since last snapshot.
	ErrNoUpdates = plainError("raft.takeSnapshot: no updates since last snapshot")

	// ErrQuorumUnreachable indicates that TransferLeadership failed because quorum of voters is unreachable.
	ErrQuorumUnreachable = plainError("raft: quorum unreachable")

	// ErrTransferNoVoter indicates that TransferLeadership task failed because number of voters in cluster is one.
	ErrTransferNoVoter = plainError("raft.transferLeadership: no other voter to transfer")

	// ErrTransferSelf indicates that TransferLeadership task failed because the target node is already leader.
	ErrTransferSelf = plainError("raft.transferLeadership: target is already leader")

	// ErrTransferTargetNonvoter indicates that TransferLeadership task failed because the target node is non-voter.
	ErrTransferTargetNonvoter = plainError("raft.transferLeadership: target is nonvoter")

	// ErrTransferInvalidTarget indicates that TransferLeadership task failed because the target node does not exist.
	ErrTransferInvalidTarget = plainError("raft.transferLeadership: no such target found")
)

var (
	errAssertion   = plainError("raft: assertion failed")
	errUnreachable = plainError("raft: unreachable")
	errInvalidTask = plainError("raft: invalid task")
	errStop        = plainError("raft: got stop signal")
)

// -----------------------------------------------------------

type bug struct {
	msg string
	err error
}

func (e bug) Error() string {
	return fmt.Sprintf("raft-bug: %s: %v", e.msg, e.err)
}

type plainError string

func (e plainError) Error() string {
	return string(e)
}

// NotLeaderError is returned by non-leader node if it cannot
// complete a request or node lost its leadership before
// completing the request.
type NotLeaderError struct {
	// Leader gives details of the leader.
	// If Leader.ID is zero, means this node does not know
	// who leader is.
	Leader Node

	// Lost is true, if the node lost its leadership before
	// completing the request.
	Lost bool
}

func (e NotLeaderError) Error() string {
	var contact string
	if e.Leader.ID != 0 {
		contact = fmt.Sprintf(", contact node %d at %s", e.Leader.ID, e.Leader.Addr)
	}
	if e.Lost {
		return "raft: lost leadership" + contact
	}
	return "raft: this node is not the leader" + contact
}

func notLeaderError(r *Raft, lost bool) NotLeaderError {
	var ldr Node
	if r.leader != 0 {
		ldr = r.configs.Latest.Nodes[r.leader]
	}
	return NotLeaderError{Leader: ldr, Lost: lost}
}

// -----------------------------------------------------------

// InProgressError is returned by leader node if it cannot
// complete a request currently because of another request
// is in progress. This is temporary error and the request
// can be retried after some time.
type InProgressError string

func (e InProgressError) Error() string {
	return fmt.Sprintf("raft: another %s in progress", string(e))
}

// Temporary flags this error as temporary and can be retried
// later.
func (e InProgressError) Temporary() {}

// TimeoutError indicates that timeout occurred in peforming a task.
// Its value represents the task name, for example "transferLeadership",
// "configChange" etc.
type TimeoutError string

func (e TimeoutError) Error() string {
	return fmt.Sprintf("raft: %s timeout", string(e))
}

// Temporary flags this error as temporary and can be retried
// later.
func (e TimeoutError) Temporary() {}

// -----------------------------------------------------------

// OpError is the error type usually returned when an error
// is detected at storage/fsm layer. This error hinders
// in raft cluster and thus needs user attention.
type OpError struct {
	// Op is the operation which caused the error, such as
	// "Log.Get" or "Snapshots.Open".
	Op string

	// Err is the error that occurred during the operation.
	Err error
}

func (e OpError) Error() string {
	return fmt.Sprintf("raft: %s: %v", e.Op, e.Err)
}

func opError(err error, format string, v ...interface{}) OpError {
	return OpError{
		Op:  fmt.Sprintf(format, v...),
		Err: err,
	}
}

type remoteError struct {
	error
}

// -----------------------------------------------------------

// IdentityError signals that identity of node at given transport
// address does not match. This happens if a node with different
// identity is running at a transport address than the specified
// node in config.
type IdentityError struct {
	Cluster uint64
	Node    uint64
	Addr    string
}

func (e IdentityError) Error() string {
	return fmt.Sprintf("raft: identity of server at %s is not cid=%d nid=%d", e.Addr, e.Cluster, e.Node)
}

// -----------------------------------------------------------

// The TemporaryError interface identifies an error that is temporary.
// This signals user to retry the operation after some time.
type TemporaryError interface {
	error

	// Temporary is a no-op function but
	// serves to distinguish errors that are temporary
	// from ordinary errors: an error is temporary
	// if it has a Temporary method.
	Temporary()
}

type temporaryError string

func (e temporaryError) Error() string {
	return string(e)
}

func (e temporaryError) Temporary() {}

// -----------------------------------------------------------

func assert(b bool) {
	if !b {
		println("barrier") // wait for tracer to finish
		panic(errAssertion)
	}
}

func unreachable() error {
	println("barrier") // wait for tracer to finish
	return errUnreachable
}

func recoverErr(r interface{}) error {
	if r == nil {
		return nil
	}
	if r == errAssertion || r == errUnreachable {
		panic(r)
	}
	switch e := r.(type) {
	case bug, runtime.Error:
		panic(e)
	case error:
		return e
	}
	return fmt.Errorf("unexpected error: %v", r)
}
