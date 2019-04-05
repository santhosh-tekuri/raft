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
)

var (
	// ErrServerClosed is returned by the Raft.Serve if it was closed due to Shutdown call
	ErrServerClosed = errors.New("raft: server closed")

	// ErrNodeRemoved is returned by the Raft.Serve if current node is removed from cluster
	ErrNodeRemoved = errors.New("raft: node removed")

	// ErrIdentityAlreadySet is returned by Storage.SetIdentity, if you are trying
	// to override current identity.
	ErrIdentityAlreadySet = errors.New("raft: identity already set")

	// ErrIdentityNotSet is returned by Raft.New, if no identity set in storage.
	ErrIdentityNotSet = errors.New("raft: identity not set")

	// ErrFaultyFollower signals that the follower is faulty, and should be
	// removed from cluster. Such follower is treated as unreachable by leader.
	// This used use by Trace.Unreachable and FlrStatus.Err.
	ErrFaultyFollower = errors.New("raft: faulty follower, denies matchIndex")

	// ErrNotCommitReady is returned by ChangeConfig, if leader is not yet ready to commit.
	// User can retry ChangeConfig after some time in case of this error.
	ErrNotCommitReady = temporaryError("raft.configChange: not ready to commit")

	ErrConfigChanged                    = errors.New("raft.configChange: config changed meanwhile")
	ErrSnapshotThreshold                = errors.New("raft.takeSnapshot: not enough outstanding logs to snapshot")
	ErrNoUpdates                        = errors.New("raft.takeSnapshot: no updates to FSM")
	ErrQuorumUnreachable                = errors.New("raft: quorum unreachable")
	ErrLeadershipTransferNoVoter        = errors.New("raft.transferLeadership: no other voter to transfer")
	ErrLeadershipTransferSelf           = errors.New("raft.transferLeadership: target is already leader")
	ErrLeadershipTransferTargetNonvoter = errors.New("raft.transferLeadership: target is nonvoter")
	ErrLeadershipTransferInvalidTarget  = errors.New("raft.transferLeadership: no such target found")
)

var (
	errInvalidTask = errors.New("raft: invalid task")
	errStop        = errors.New("raft: got stop signal")
)

// -----------------------------------------------------------

// NotLeaderError is returned by non-leader node if it cannot
// complete a request or node lost its leadership before
// completing the request.
type NotLeaderError struct {
	// LeaderID is the nodeID of leader.
	//
	// It is zero, if this node does not know who leader is.
	LeaderID uint64

	// LeaderAddr is address of leader.
	//
	// It is empty string, if this node does not know who leader is.
	LeaderAddr string

	// Lost is true, if the node lost its leadership before
	// completing the request.
	Lost bool
}

func (e NotLeaderError) Error() string {
	var contact string
	if e.LeaderID != 0 {
		contact = fmt.Sprintf(", contact node %d at %s", e.LeaderID, e.LeaderAddr)
	}
	if e.Lost {
		return "raft: lost leadership" + contact
	}
	return "raft: this node is not the leader" + contact
}

// -----------------------------------------------------------

type InProgressError string

func (e InProgressError) Error() string {
	return fmt.Sprintf("raft: another %s in progress", string(e))
}

func (e InProgressError) Temporary() {}

type TimeoutError string

func (e TimeoutError) Error() string {
	return fmt.Sprintf("raft: %s timeout", string(e))
}

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
		panic("assertion failed")
	}
}

func unreachable() {
	println("barrier") // wait for tracer to finish
	panic("unreachable")
}
