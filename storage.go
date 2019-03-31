package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"sync"
)

type Vars interface {
	GetIdentity() (cid, nid uint64, err error)
	SetIdentity(cid, nid uint64) error
	GetVote() (term, vote uint64, err error)
	SetVote(term, vote uint64) error
}

type Log interface {
	Count() (uint64, error)
	Get(offset uint64) ([]byte, error)
	WriteTo(w io.Writer, offset uint64, n uint64) error
	Append(entry []byte) error
	DeleteFirst(n uint64) error
	DeleteLast(n uint64) error
}

// -----------------------------------------------------------------------------------

// Storage contains all persistent state.
type Storage struct {
	Vars Vars
	Log  Log
	Dir  string
}

// GetIdentity returns the server identity.
//
// The identity includes clusterID and nodeID. Zero values
// mean identity is not set yet.
func (s Storage) GetIdentity() (cid, nid uint64, err error) {
	cid, nid, err = s.Vars.GetIdentity()
	if err != nil {
		err = opError(err, "Vars.GetIdentity")
	}
	return
}

// SetIdentity sets the server identity.
//
// If identity is already set and you are trying
// to override it with different identity, it returns error.
func (s Storage) SetIdentity(cid, nid uint64) error {
	if cid == 0 {
		return errors.New("raft: cid is zero")
	}
	if nid == 0 {
		return errors.New("raft: nid is zero")
	}
	cluster, node, err := s.GetIdentity()
	if err != nil {
		return err
	}
	if cid == cluster && nid == node {
		return nil
	}
	if cluster != 0 || node != 0 {
		return ErrIdentityAlreadySet
	}
	err = s.Vars.SetIdentity(cid, nid)
	if err != nil {
		err = opError(err, "Vars.SetIdentity")
	}
	return nil
}

type storage struct {
	vars     Vars
	cid      uint64
	nid      uint64
	term     uint64
	votedFor uint64

	log          Log
	prevLogMu    sync.RWMutex
	prevLogIndex uint64
	lastLogIndex uint64
	lastLogTerm  uint64

	snaps   *snapshots
	configs Configs
}

func newStorage(s Storage) *storage {
	snaps := &snapshots{dir: filepath.Join(s.Dir, "snapshots")}
	return &storage{
		vars:  s.Vars,
		log:   s.Log,
		snaps: snaps,
	}
}

func (s *storage) init() error {
	var err error

	// init identity
	s.cid, s.nid, err = s.vars.GetIdentity()
	if err != nil {
		return opError(err, "Vars.GetIdentity")
	}
	if s.cid == 0 || s.nid == 0 {
		return ErrIdentityNotSet
	}

	// init vars ---------------------
	s.term, s.votedFor, err = s.vars.GetVote()
	if err != nil {
		return opError(err, "Vars.GetVote")
	}

	// init snapshots ---------------
	if err = s.snaps.init(); err != nil {
		return opError(err, "snapshots.init")
	}
	meta, err := s.snaps.meta()
	if err != nil {
		return opError(err, "snapshots.meta")
	}

	// init log ---------------------
	count, err := s.log.Count()
	if err != nil {
		return opError(err, "Log.Count")
	}
	if count == 0 {
		s.lastLogIndex, s.lastLogTerm = s.snaps.index, s.snaps.term
		s.prevLogIndex = s.snaps.index
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
	for i := s.lastLogIndex; i > s.snaps.index; i-- {
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

func (s *storage) setVotedFor(term, votedFor uint64) {
	if term < s.term {
		panic(fmt.Sprintf("term cannot be changed from %d to %d", s.term, term))
	}
	if err := s.vars.SetVote(s.term, votedFor); err != nil {
		panic(opError(err, "Vars.SetVote(%d, %d)", s.term, votedFor))
	}
	s.term, s.votedFor = term, votedFor
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
	if index <= s.prevLogIndex {
		return errNoEntryFound
	}
	offset := index - s.prevLogIndex - 1
	b, err := s.log.Get(offset)
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

func (s *storage) mustGetEntry(index uint64, e *entry) {
	if err := s.getEntry(index, e); err != nil {
		panic(bug(2, "storage.MustGetEntry(%d): %v", index, err))
	}
}

func (s *storage) WriteEntriesTo(w io.Writer, from uint64, n uint64) error {
	if from <= s.prevLogIndex {
		return errNoEntryFound
	}
	offset := from - s.prevLogIndex - 1
	if err := s.log.WriteTo(w, offset, n); err != nil {
		if _, ok := err.(*net.OpError); !ok {
			panic(opError(err, "Log.WriteTo(%d, %d)", offset, n))
		}
		return err
	}
	return nil
}

// called by raft.runLoop. getEntry call can be called during this
func (s *storage) appendEntry(e *entry) {
	if e.index != s.lastLogIndex+1 {
		panic(bug(2, "storage.appendEntry.index: got %d, want %d", e.index, s.lastLogIndex+1))
	}
	w := new(bytes.Buffer)
	if err := e.encode(w); err != nil {
		panic(bug(2, "entry.encode(%d): %v", e.index, err))
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
	s.prevLogMu.Lock()
	defer s.prevLogMu.Unlock()
	count := s.lastLogIndex - s.prevLogIndex
	if err := s.log.DeleteFirst(count); err != nil {
		return err
	}
	s.lastLogIndex, s.lastLogTerm = s.snaps.index, s.snaps.term
	s.prevLogIndex = s.snaps.index
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
			if _, ok := v.(runtime.Error); ok {
				panic(v)
			}
			err = toErr(v)
		}
	}()
	s.appendEntry(config.encode())
	s.setTerm(1)
	return nil
}
