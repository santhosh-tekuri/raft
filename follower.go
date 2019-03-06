package raft

import "time"

func (r *Raft) runFollower() {
	f := flrShip{Raft: r}
	f.init()
	f.runLoop()
	f.release()
}

type flrShip struct {
	*Raft
	timeoutCh       <-chan time.Time
	electionAborted bool
}

func (f *flrShip) init() {
	f.timeoutCh = afterRandomTimeout(f.hbTimeout)
	f.electionAborted = false
}

func (f *flrShip) release() {
	f.timeoutCh = nil
}

func (f *flrShip) runLoop() {
	assert(f.leader != f.id, "%s r.leader: got %s, want !=%s", f, f.leader, f.id)

	// todo: use single timer by resetting
	for f.state == Follower {
		select {
		case <-f.shutdownCh:
			return

		case rpc := <-f.server.rpcCh:
			// a server remains in follower state as long as it receives valid
			// RPCs from a leader or candidate
			if validReq := f.replyRPC(rpc); validReq {
				f.resetTimer()
			}

			// If timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
		case <-f.timeoutCh:
			f.onTimeout()

		case ne := <-f.newEntryCh:
			ne.reply(NotLeaderError{f.leaderAddr(), false})

		case t := <-f.taskCh:
			f.executeTask(t)
			if f.electionAborted {
				f.resetTimer()
			}

		case t := <-f.snapTakenCh:
			f.onSnapshotTaken(t)
		}
	}
}

func (f *flrShip) resetTimer() {
	if yes, _ := f.canStartElection(); yes {
		f.electionAborted = false
		f.timeoutCh = afterRandomTimeout(f.hbTimeout)
	}
}

func (f *flrShip) onTimeout() {
	debug(f, "heartbeatTimeout leader:", f.leader)
	f.leader = ""

	if can, reason := f.canStartElection(); !can {
		debug(f, "electionAborted", reason)
		f.electionAborted = true
		if f.trace.ElectionAborted != nil {
			f.trace.ElectionAborted(f.liveInfo(), reason)
		}
		return
	}

	debug(f, "follower -> candidate")
	f.state = Candidate
	f.stateChanged()
}

func (f *flrShip) canStartElection() (can bool, reason string) {
	if f.configs.IsBootstrap() {
		return false, "no known peers"
	}
	if f.configs.IsCommitted() {
		n, ok := f.configs.Latest.Nodes[f.id]
		if !ok {
			return false, "not part of cluster"
		}
		if !n.Voter {
			return false, "not voter"
		}
	}
	return true, ""
}
