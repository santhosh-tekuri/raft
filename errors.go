package raft

import (
	"errors"
	"fmt"
)

var (
	// ErrServerClosed is returned by the Raft's Serve and ListenAndServe
	// methods after a call to Shutdown
	ErrServerClosed = errors.New("raft: server closed")

	// ErrAlreadyBootstrapped is returned when bootstrap task received
	// by already bootstrapped server
	ErrAlreadyBootstrapped              = errors.New("raft.bootstrap: already bootstrapped")
	ErrNotCommitReady                   = errors.New("raft.configChange: not ready to commit")
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
	errInvalidTask  = errors.New("raft: invalid task")
	errNoEntryFound = errors.New("raft: no entry found")
	errStop         = errors.New("raft: got stop signal")
)

// -----------------------------------------------------------

// NotLeaderError is returned by non-leader node if it cannot
// complete a request or node lost its leadership before
// completing the request.
type NotLeaderError struct {
	// LeaderAddr is address of leader.
	//
	// It is empty string, if this node does not know current leader.
	LeaderAddr string

	// Lost is true, if the node lost its leadership before
	// completing the request.
	Lost bool
}

func (e NotLeaderError) Error() string {
	var contact string
	if e.LeaderAddr != "" {
		contact = ", contact " + e.LeaderAddr
	}
	if e.Lost {
		return "raft: lost leadership " + contact
	}
	return "raft: this node is not the leader" + contact
}

// -----------------------------------------------------------

type InProgressError string

func (e InProgressError) Error() string {
	return fmt.Sprintf("raft: another %s in progress", string(e))
}

type TimeoutError string

func (e TimeoutError) Error() string {
	return fmt.Sprintf("raft: %s timeout", string(e))
}

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
