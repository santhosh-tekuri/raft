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
	"time"
)

func (l *leader) onChangeConfig(t changeConfig) {
	if !l.configs.IsCommitted() {
		t.reply(InProgressError("configChange"))
		return
	}
	// see https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
	if l.commitIndex < l.startIndex {
		t.reply(ErrNotCommitReady)
		return
	}
	if t.newConf.Index != l.configs.Latest.Index {
		t.reply(ErrStaleConfig)
		return
	}
	if err := t.newConf.validate(); err != nil {
		t.reply(fmt.Errorf("raft.changeConfig: %v", err))
		return
	}

	// ensure that except action, address nothing is modified
	for id, n := range l.configs.Latest.Nodes {
		nn, ok := t.newConf.Nodes[id]
		if !ok {
			t.reply(fmt.Errorf("raft.changeConfig: node %d is removed", id))
			return
		}
		if n.Voter != nn.Voter {
			t.reply(fmt.Errorf("raft.changeConfig: node %d voting right changed", id))
			return
		}
	}
	for id, n := range t.newConf.Nodes {
		if _, ok := l.configs.Latest.Nodes[id]; !ok {
			if n.Voter {
				t.reply(fmt.Errorf("raft.changeConfig: new node %d must be nonvoter", id))
				return
			}
		}
	}

	// ensure that new cluster will have at least one voter
	var voter uint64
	for id, n := range t.newConf.Nodes {
		if n.Voter && n.Action == None {
			voter = id
		}
	}
	if voter == 0 {
		t.reply(fmt.Errorf("raft.changeConfig: at least one voter must remain in cluster"))
		return
	}

	l.checkConfigActions(t.task, t.newConf)
	if l.configs.IsCommitted() {
		if trace {
			println(l, "no configActions changed")
		}
		// no actions changed, so commit as it is
		l.doChangeConfig(t.task, t.newConf)
	}
}

func (l *leader) doChangeConfig(t *task, config Config) {
	l.storeEntry(&newEntry{
		entry: config.encode(),
		task:  t,
	})
}

// called from leader.storeEntry, if lastLogIndex is updated
func (l *leader) beginFinishedRounds() {
	for id, repl := range l.repls {
		r := repl.status.round
		if r != nil && r.finished() {
			r.begin(l.lastLogIndex)
			if trace {
				println(l, id, "started:", r)
			}
		}
	}
}

// checkConfigActions finishes any postponed promotions
//
// this is called:
// - from leader.init
// - from leader.changeConfig
// - from leader.setCommitIndex, if config is committed
// - from leader.onTransferTimeout
func (l *leader) checkConfigActions(t *task, config Config) {
	// do actions on self if any
	n := config.Nodes[l.nid]
	if l.canChangeConfig() && n.Action != None {
		if trace {
			println(l, n.ID, "started", n.Action)
		}
		if l.tracer.ConfigActionStarted != nil {
			l.tracer.ConfigActionStarted(l.liveInfo(), n.ID, n.Action)
		}
		switch n.Action {
		case Demote:
			config = config.clone()
			n.Voter = false
			if n.Action == Demote {
				n.Action = None
			}
			config.Nodes[n.ID] = n
		case Remove, ForceRemove:
			config = config.clone()
			delete(config.Nodes, l.nid)
		default:
			panic(unreachable())
		}
		l.doChangeConfig(t, config)
	}

	for _, repl := range l.repls {
		l.checkConfigAction(t, config, &repl.status)
	}
}

// checks whether round is completed, if so
// promotes if threshold is satisfied.
//
// - from leader.checkReplUpdates, if repl.matchIndex is updated
// - from leader.checkConfigActions
func (l *leader) checkConfigAction(t *task, config Config, status *replicationStatus) {
	n := config.Nodes[status.id]
	action := n.nextAction()
	if action == None {
		return
	}

	// start or stop rounds
	if action != Promote {
		status.round = nil
	} else if status.round == nil {
		// start first round
		status.round = new(Round)
		status.round.begin(l.lastLogIndex)
		if trace {
			println(l, status.id, "started:", status.round)
		}
	}

	// finish round if completed, start new round if necessary
	if status.round != nil {
		r := status.round
		if !r.finished() && status.matchIndex >= r.LastIndex {
			r.finish()
			if trace {
				println(l, status.id, "finished:", r)
			}
			l.logger.Info("nonVoter", status.id, "completed round", r.Ordinal, "in", r.Duration(), ", its lastIndex:", r.LastIndex)
			if l.tracer.RoundCompleted != nil {
				l.tracer.RoundCompleted(l.liveInfo(), status.id, *r)
			}
		}
		if !r.finished() {
			return
		}
		hasNewEntries := l.lastLogIndex > status.matchIndex
		if hasNewEntries && r.Duration() > l.promoteThreshold {
			r.begin(l.lastLogIndex)
			if trace {
				println(l, status.id, "started:", r)
			}
			return
		}
	}

	if !l.canChangeConfig() {
		if trace {
			println(l, status.id, "cannot", action, "now")
		}
		return
	}

	// perform configAction
	switch action {
	case Promote:
		l.logger.Info("promoting nonvoter ", n.ID, ", after", status.round.Ordinal, "round(s)")
		config = config.clone()
		n.Voter, n.Action = true, None
		config.Nodes[n.ID] = n
	case Remove:
		if status.matchIndex >= l.configs.Latest.Index {
			l.logger.Info("removing nonvoter", n.ID)
			config = config.clone()
			delete(config.Nodes, n.ID)
		} else {
			return
		}
	case ForceRemove:
		l.logger.Info("force removing node", n.ID)
		config = config.clone()
		delete(config.Nodes, n.ID)
	case Demote:
		l.logger.Info("demoting voter", n.ID)
		config = config.clone()
		n.Voter = false
		if n.Action == Demote {
			n.Action = None
		}
		config.Nodes[n.ID] = n
	}
	if trace {
		println(l, status.id, "started", action)
	}
	if l.tracer.ConfigActionStarted != nil {
		l.tracer.ConfigActionStarted(l.liveInfo(), n.ID, action)
	}
	l.doChangeConfig(t, config)
}

func (l *leader) canChangeConfig() bool {
	return l.configs.IsCommitted() && !l.transfer.inProgress()
}

func (l *leader) onWaitForStableConfig(t waitForStableConfig) {
	if l.configs.IsStable() {
		t.reply(l.configs.Latest)
		return
	}
	l.waitStable = append(l.waitStable, t)
}

// Round ------------------------------------------------

type Round struct {
	Ordinal   uint64
	Start     time.Time
	End       time.Time
	LastIndex uint64
}

func (r *Round) begin(lastIndex uint64) {
	r.Ordinal, r.Start, r.LastIndex = r.Ordinal+1, time.Now(), lastIndex
}
func (r *Round) finish()                { r.End = time.Now() }
func (r *Round) finished() bool         { return !r.End.IsZero() }
func (r Round) Duration() time.Duration { return r.End.Sub(r.Start) }

func (r Round) String() string {
	if r.finished() {
		return fmt.Sprintf("Round{#%d %s lastIndex: %d}", r.Ordinal, r.Duration(), r.LastIndex)
	}
	return fmt.Sprintf("Round{#%d lastIndex: %d}", r.Ordinal, r.LastIndex)
}
