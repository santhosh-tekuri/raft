package raft

import (
	"errors"
	"io"
	"time"
)

type member struct {
	rtime randTime

	// this is owned by ldr goroutine
	status memberStatus

	connPool  *connPool
	storage   *storage
	hbTimeout time.Duration
	conn      *netConn

	ldrStartIndex uint64
	ldrLastIndex  uint64
	matchIndex    uint64
	nextIndex     uint64
	sendEntries   bool

	node       Node
	round      uint64
	roundStart time.Time
	roundEnd   uint64

	// from this time node is unreachable
	// zero value means node is reachable
	noContact time.Time

	// leader notifies member with update
	fromLeaderCh chan leaderUpdate

	// member notifies leader about its progress
	toLeaderCh chan<- interface{}
	stopCh     chan struct{}

	trace *Trace
	str   string // used for debug() calls
}

func (m *member) replicate(req *appendEntriesReq) {
	defer func() {
		if m.conn != nil {
			m.connPool.returnConn(m.conn)
		}
	}()

	m.ldrLastIndex = req.prevLogIndex
	m.matchIndex, m.nextIndex = uint64(0), m.ldrLastIndex+1
	if m.node.promote() {
		m.round, m.roundStart, m.roundEnd = 0, time.Now(), m.ldrLastIndex
		debug(m, "starting round:", m.round, "roundEnd:", m.roundEnd)
	}

	debug(m, "m.start")
	for {
		debug(m, "matchIndex", m.matchIndex, "nextIndex", m.nextIndex)
		assert(m.matchIndex < m.nextIndex, "%s assert %d<%d", m, m.matchIndex, m.nextIndex)

		err := m.sendAppEntriesReq(req)
		if err == errNoEntryFound {
			err = m.sendInstallSnapReq(req)
		}

		if err == errStop {
			return
		} else if err != nil {
			if m.trace.Error != nil {
				m.trace.Error(err)
			}
			assert(false, "unexpected error: %v", err) // todo
			continue
		}

		if m.sendEntries && m.matchIndex == m.ldrLastIndex {
			// nothing to send. start heartbeat timer
			select {
			case <-m.stopCh:
				return
			case update := <-m.fromLeaderCh:
				m.ldrLastIndex, req.ldrCommitIndex = update.lastIndex, update.commitIndex
				debug(m, "{last:", m.ldrLastIndex, "commit:", req.ldrCommitIndex, "} <-fromLeaderCh")
			case <-m.rtime.after(m.hbTimeout / 10):
			}
		} else {
			// check signal if any, without blocking
			select {
			case <-m.stopCh:
				return
			case update := <-m.fromLeaderCh:
				m.ldrLastIndex, req.ldrCommitIndex = update.lastIndex, update.commitIndex
				debug(m, "{last:", m.ldrLastIndex, "commit:", req.ldrCommitIndex, "} <-fromLeaderCh")
			default:
			}
		}
	}
}

func (m *member) onLeaderUpdate(update leaderUpdate, req *appendEntriesReq) {
	m.ldrLastIndex, req.ldrCommitIndex = update.lastIndex, update.commitIndex
	debug(m, "{last:", m.ldrLastIndex, "commit:", req.ldrCommitIndex, "} <-fromLeaderCh")

	// if promote is changed to true, start first round
	if update.config != nil {
		if n, ok := update.config.Nodes[m.status.id]; ok {
			if n.promote() && !m.node.promote() {
				// start first round
				m.round, m.roundStart, m.roundEnd = 0, time.Now(), m.ldrLastIndex
				debug(m, "starting round:", m.round, "roundEnd:", m.roundEnd)
				if m.matchIndex >= m.roundEnd {
					m.roundCompleted()
				}
			}
			m.node = n
		}
	}
}

var errStop = errors.New("got stop signal")

func (m *member) sendAppEntriesReq(req *appendEntriesReq) error {
	req.prevLogIndex = m.nextIndex - 1

	// fill req.prevLogTerm
	if req.prevLogIndex == 0 {
		req.prevLogTerm = 0
	} else {
		snapIndex, snapTerm := m.getSnapLog()
		if req.prevLogIndex < snapTerm {
			return errNoEntryFound
		}
		if req.prevLogIndex == snapIndex {
			req.prevLogTerm = snapTerm
		} else if req.prevLogIndex >= m.ldrStartIndex { // being smart!!!
			req.prevLogTerm = req.term
		} else {
			prevTerm, err := m.storage.getEntryTerm(req.prevLogIndex)
			if err != nil {
				return err
			}
			req.prevLogTerm = prevTerm
		}
	}

	var n uint64
	if m.sendEntries {
		assert(m.matchIndex == req.prevLogIndex, "%s assert %d==%d", m, m.matchIndex, req.prevLogIndex)
		n = min(m.ldrLastIndex-m.matchIndex, maxAppendEntries)
	}
	if n > 0 {
		req.entries = make([]*entry, n)
		for i := range req.entries {
			req.entries[i] = &entry{}
			err := m.storage.getEntry(m.nextIndex+uint64(i), req.entries[i])
			if err != nil {
				return err
			}
		}
		debug(m, "sending", req)
	} else {
		req.entries = nil
		if m.sendEntries {
			debug(m, "sending heartbeat")
		}
	}

	resp := &appendEntriesResp{}
	if err := m.retryRPC(req, resp); err != nil {
		return err
	}

	m.sendEntries = resp.success
	if resp.success {
		old := m.matchIndex
		m.matchIndex, _ = lastEntry(req)
		m.nextIndex = m.matchIndex + 1
		if m.matchIndex != old {
			debug(m, "matchIndex:", m.matchIndex)
			m.notifyLdr(matchIndex{&m.status, m.matchIndex})
		}
	} else {
		if resp.lastLogIndex < m.matchIndex {
			// this happens if someone restarted follower storage with empty storage
			// todo: can we treat replicate entire snap+log to such follower ??
			return errors.New("faulty follower: denies matchIndex")
		}
		m.nextIndex = min(m.nextIndex-1, resp.lastLogIndex+1)
		debug(m, "nextIndex:", m.nextIndex)
	}
	return nil
}

func (m *member) sendInstallSnapReq(appReq *appendEntriesReq) error {
	req := &installLatestSnapReq{
		installSnapReq: installSnapReq{
			term:   appReq.term,
			leader: appReq.leader,
		},
		snapshots: m.storage.snapshots,
	}

	resp := &installSnapResp{}
	if err := m.retryRPC(req, resp); err != nil {
		return err
	}

	// we have to still send one appEntries, to update his commit index
	// so we should not update sendEntries=true, beacuse if we have
	// no entries beyond snapshot, we sleep for hbTimeout
	//m.sendEntries = resp.success // NOTE: dont do this
	if resp.success {
		m.matchIndex = req.lastIndex
		m.nextIndex = m.matchIndex + 1
		debug(m, "matchIndex:", m.matchIndex)
		m.notifyLdr(matchIndex{&m.status, m.matchIndex})
		return nil
	} else {
		return errors.New("installSnap.success is false")
	}
}

func (m *member) retryRPC(req request, resp message) error {
	var failures uint64
	for {
		err := m.doRPC(req, resp)
		if _, ok := err.(OpError); ok {
			return err
		} else if err != nil {
			if m.noContact.IsZero() {
				m.noContact = time.Now()
				debug(m, "noContact", err)
				m.notifyLdr(noContact{&m.status, m.noContact})
			}
			failures++
			select {
			case <-m.stopCh:
				return errStop
			case <-time.After(backOff(failures)):
				continue
			}
		}
		break
	}
	if !m.noContact.IsZero() {
		m.noContact = time.Time{} // zeroing
		debug(m, "yesContact")
		m.notifyLdr(noContact{&m.status, m.noContact})
	}
	if resp.getTerm() > req.getTerm() {
		m.notifyLdr(newTerm{resp.getTerm()})
		return errStop
	}
	return nil
}

func (m *member) doRPC(req request, resp message) error {
	if m.conn == nil {
		conn, err := m.connPool.getConn()
		if err != nil {
			return err
		}
		m.conn = conn
	}
	if m.trace.sending != nil {
		m.trace.sending(req.from(), m.connPool.id, req)
	}
	err := m.conn.doRPC(req, resp)
	if err != nil {
		_ = m.conn.close()
		m.conn = nil
	}
	if m.trace.sending != nil && err == nil {
		m.trace.received(req.from(), m.connPool.id, resp)
	}
	return err
}

func (m *member) notifyLdr(update interface{}) {
	select {
	case <-m.stopCh:
	case m.toLeaderCh <- update:
	}

	// check if we just completed round
	if _, ok := update.(matchIndex); ok && m.node.promote() {
		if m.matchIndex >= m.roundEnd {
			m.roundCompleted()
		}
	}
}

func (m *member) roundCompleted() {
	update := roundCompleted{&m.status, m.round, m.roundEnd, time.Now().Sub(m.roundStart)}
	select {
	case <-m.stopCh:
	case m.toLeaderCh <- update:
	}
	// prepare next round
	m.round, m.roundStart, m.roundEnd = m.round+1, time.Now(), m.ldrLastIndex
}

func (m *member) getSnapLog() (snapIndex, snapTerm uint64) {
	// snapshoting might be in progress
	m.storage.snapMu.RLock()
	defer m.storage.snapMu.RUnlock()
	return m.storage.snapIndex, m.storage.snapTerm
}

func (m *member) String() string {
	return m.str
}

// ------------------------------------------------

type installLatestSnapReq struct {
	installSnapReq
	snapshots Snapshots
}

func (req *installLatestSnapReq) encode(w io.Writer) error {
	meta, snapshot, err := req.snapshots.Open()
	if err != nil {
		return opError(err, "Snapshots.Open")
	}
	req.lastIndex = meta.Index
	req.lastTerm = meta.Term
	req.lastConfig = meta.Config
	req.size = meta.Size
	req.snapshot = snapshot
	return req.installSnapReq.encode(w)
}

type leaderUpdate struct {
	lastIndex   uint64
	commitIndex uint64
	config      *Config // nil if config not changed
}

type matchIndex struct {
	status *memberStatus
	val    uint64
}

type noContact struct {
	status *memberStatus
	time   time.Time
}

type newTerm struct {
	val uint64
}

type roundCompleted struct {
	status    *memberStatus
	round     uint64
	lastIndex uint64
	duration  time.Duration
}

type memberStatus struct {
	id ID

	// owned exclusively by leader goroutine
	// used to compute majorityMatchIndex
	matchIndex uint64

	// from what time the replication unable to reach this node
	// zero value means it is reachable
	noContact time.Time
}

// did we have success full contact after time t
func (rs *memberStatus) contactedAfter(t time.Time) bool {
	return rs.noContact.IsZero() || rs.noContact.After(t)
}

// ------------------------------------------------

const (
	maxAppendEntries = 64 // todo: should be configurable
	maxFailureScale  = 12
	failureWait      = 10 * time.Millisecond
)

// backOff is used to compute an exponential backOff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor.
func backOff(round uint64) time.Duration {
	base, limit := failureWait, uint64(maxFailureScale)
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	return base
}

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
