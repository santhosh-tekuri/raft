package raft

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
)

type Vars interface {
	GetIdentity() (cluster, node uint64, err error)
	SetIdentity(cluster, node uint64) error
	GetVote() (term, vote uint64, err error)
	SetVote(term, vote uint64) error
}

type Log interface {
	Count() (uint64, error)
	Get(offset uint64) ([]byte, error)
	Append(entry []byte) error
	DeleteFirst(n uint64) error
	DeleteLast(n uint64) error
}

type Snapshots interface {
	New(index, term uint64, config Config) (SnapshotSink, error)
	Meta() (SnapshotMeta, error)
	Open() (SnapshotMeta, io.ReadCloser, error)
}

type SnapshotMeta struct {
	Index  uint64
	Term   uint64
	Config Config
	Size   int64
}

type SnapshotSink interface {
	io.Writer
	Done(err error) (SnapshotMeta, error)
}

// -----------------------------------------------------------------------------------

type Storage struct {
	Vars      Vars
	Log       Log
	Snapshots Snapshots
}

func (s Storage) GetIdentity() (cluster, node uint64, err error) {
	cluster, node, err = s.Vars.GetIdentity()
	if err != nil {
		err = opError(err, "Vars.GetIdentity")
	}
	return
}

func (s Storage) SetIdentity(cluster, node uint64) error {
	if cluster == 0 {
		return errors.New("raft: cluster is zero")
	}
	if node == 0 {
		return errors.New("raft: node is zero")
	}
	cid, nid, err := s.GetIdentity()
	if err != nil {
		return err
	}
	if cid == cluster && nid == node {
		return nil
	}
	if cid != 0 || nid != 0 {
		return ErrIdentityAlreadySet
	}
	err = s.Vars.SetIdentity(cluster, node)
	if err != nil {
		err = opError(err, "Vars.SetIdentity")
	}
	return nil
}

// todo: can we avoid panics on storage error
type storage struct {
	vars     Vars
	cid      uint64
	id       uint64
	term     uint64
	votedFor uint64

	log          Log
	prevLogMu    sync.RWMutex
	prevLogIndex uint64
	lastLogIndex uint64
	lastLogTerm  uint64

	snapshots Snapshots
	snapMu    sync.RWMutex
	snapIndex uint64
	snapTerm  uint64

	configs Configs
}

func newStorage(s Storage) *storage {
	return &storage{
		vars:      s.Vars,
		log:       s.Log,
		snapshots: s.Snapshots,
	}
}

func (s *storage) init() error {
	var err error

	// init identity
	s.cid, s.id, err = s.vars.GetIdentity()
	if err != nil {
		return opError(err, "Vars.GetIdentity")
	}
	if s.cid == 0 || s.id == 0 {
		return ErrIdentityNotSet
	}

	// init vars ---------------------
	s.term, s.votedFor, err = s.vars.GetVote()
	if err != nil {
		return opError(err, "Vars.GetVote")
	}

	// init snapshots ---------------
	meta, err := s.snapshots.Meta()
	if err != nil {
		return opError(err, "Snapshots.Meta")
	}
	s.snapIndex, s.snapTerm = meta.Index, meta.Term

	// init log ---------------------
	count, err := s.log.Count()
	if err != nil {
		return opError(err, "Log.Count")
	}
	if count == 0 {
		s.lastLogIndex, s.lastLogTerm = s.snapIndex, s.snapTerm
		s.prevLogIndex = s.snapIndex
	} else {
		data, err := s.log.Get(count - 1)
		if err != nil {
			return opError(err, "Log.Get(%d)", count-1)
		}
		e := &entry{}
		if err := e.decode(bytes.NewReader(data)); err != nil {
			return opError(err, "Log.Get(%d).decode", count-1)
		}
		s.lastLogIndex, s.lastLogTerm = e.index, e.term
		s.prevLogIndex = s.lastLogIndex - count
	}

	// load configs ----------------
	need := 2
	for i := s.lastLogIndex; i > s.snapIndex; i-- {
		e := &entry{}
		err = s.getEntry(i, e)
		if err != nil {
			return err
		}
		if e.typ == entryConfig {
			if need == 2 {
				err = s.configs.Latest.decode(e)
			} else {
				err = s.configs.Committed.decode(e)
			}
			if err != nil {
				return err
			}
			need--
			if need == 0 {
				break
			}
		}
	}
	if need == 2 {
		s.configs.Latest = meta.Config
		need--
	}
	if need == 1 {
		s.configs.Committed = meta.Config
	}

	return nil
}

func (s *storage) setTerm(term uint64) {
	if s.term != term {
		if term < s.term {
			panic(fmt.Sprintf("term cannot be changed from %d to %d", s.term, term))
		}
		if err := s.vars.SetVote(s.term, 0); err != nil {
			panic(opError(err, "Vars.SetVote(%d, %d)", term, 0))
		}
		s.term, s.votedFor = term, 0
	}
}

func (s *storage) setVotedFor(id uint64) {
	if id == 0 {
		panic(bug("setVotedFor(0)"))
	}
	if err := s.vars.SetVote(s.term, id); err != nil {
		panic(opError(err, "Vars.SetVote(%d, %d)", s.term, id))
	}
	s.votedFor = id
}

// NOTE: this should not be called with snapIndex
func (s *storage) getEntryTerm(index uint64) (uint64, error) {
	e := &entry{}
	err := s.getEntry(index, e)
	return e.term, err
}

// called by raft.runLoop and m.replicate. append call can be called during this
// never called with invalid index
func (s *storage) getEntry(index uint64, e *entry) error {
	s.prevLogMu.RLock()
	if index <= s.prevLogIndex {
		return errNoEntryFound
	}
	offset := index - s.prevLogIndex - 1
	b, err := s.log.Get(offset)
	s.prevLogMu.RUnlock()
	if err != nil {
		panic(opError(err, "Log.Get(%d)", offset))
	}
	if err = e.decode(bytes.NewReader(b)); err != nil {
		panic(opError(err, "log.Get(%d).decode()", offset))
	}
	if e.index != index {
		panic(opError(fmt.Errorf("got %d, want %d", e.index, index), "log.Get(%d).index: ", offset))
	}
	return nil
}

// called by raft.runLoop. getEntry call can be called during this
func (s *storage) appendEntry(e *entry) {
	if e.index != s.lastLogIndex+1 {
		panic(bug("storage.appendEntry.index: got %d, want %d", e.index, s.lastLogIndex+1))
	}
	w := new(bytes.Buffer)
	if err := e.encode(w); err != nil {
		panic(bug("entry.encode(%d): %v", e.index, err))
	}
	if err := s.log.Append(w.Bytes()); err != nil {
		panic(opError(err, "Log.Append"))
	}
	s.lastLogIndex, s.lastLogTerm = e.index, e.term
}

// never called with invalid index
func (s *storage) deleteLTE(index uint64) error {
	s.prevLogMu.Lock()
	defer s.prevLogMu.Unlock()
	debug("deleteLTE index:", index, "prevLogIndex:", s.prevLogIndex, "lastLogIndex:", s.lastLogIndex)
	n := index - s.prevLogIndex
	if err := s.log.DeleteFirst(n); err != nil {
		return opError(err, "Log.DeleteFirst(%d)", n)
	}
	s.prevLogIndex = index
	return nil
}

// no flr.replicate is going on when this called
func (s *storage) clearLog() error {
	count := s.lastLogIndex - s.prevLogIndex
	if err := s.log.DeleteFirst(count); err != nil {
		return err
	}
	s.lastLogIndex, s.lastLogTerm = s.snapIndex, s.snapTerm
	s.prevLogIndex = s.snapIndex
	return nil
}

// called by raft.runLoop. no other calls made during this
// never called with invalid index
func (s *storage) deleteGTE(index, prevTerm uint64) {
	n := s.lastLogIndex - index + 1
	if err := s.log.DeleteLast(n); err != nil {
		panic(opError(err, "Log.DeleteLast(%d)", n))
	}
	s.lastLogIndex, s.lastLogTerm = index-1, prevTerm
}

func (s *storage) bootstrap(config Config) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = toErr(v)
		}
	}()
	s.appendEntry(config.encode())
	s.setTerm(1)
	return nil
}
