package raft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

type Vars interface {
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

// todo: can we avoid panics on storage error
type storage struct {
	vars     Vars
	term     uint64
	votedFor uint64

	log          Log
	lastLogIndex uint64
	lastLogTerm  uint64

	snapshots Snapshots
	snapMu    sync.RWMutex // todo
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
	s.lastLogIndex = s.snapIndex + count
	if count == 0 {
		s.lastLogTerm = s.snapTerm
	} else {
		s.lastLogTerm, err = s.getEntryTerm(s.lastLogIndex)
		if err != nil {
			return err
		}
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
			panic(opError(err, "Vars.SetVote(%d, %q)", s.term, ""))
		}
		s.term, s.votedFor = term, 0
	}
}

func (s *storage) setVotedFor(id uint64) {
	if id == 0 {
		panic("setVotedFor(0)")
	}
	if err := s.vars.SetVote(s.term, id); err != nil {
		panic(opError(err, "Vars.SetVote(%d, %q)", s.term, string(id)))
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
	s.snapMu.RLock()
	if index <= s.snapIndex {
		return errNoEntryFound
	}
	offset := index - s.snapIndex - 1
	b, err := s.log.Get(offset)
	s.snapMu.RUnlock()
	if err != nil {
		panic(opError(err, "Log.Get(%d)", offset))
	}
	if err = e.decode(bytes.NewReader(b)); err != nil {
		panic(fmt.Sprintf("raft: entry.decode(%d) failed: %v", index, err))
	}
	return nil
}

// called by raft.runLoop. getEntry call can be called during this
func (s *storage) appendEntry(e *entry) {
	w := new(bytes.Buffer)
	if err := e.encode(w); err != nil {
		panic(fmt.Sprintf("raft: entry.encode(%d) failed: %v", e.index, err))
	}
	if err := s.log.Append(w.Bytes()); err != nil {
		panic(opError(err, "Log.Append"))
	}
	if e.index != s.lastLogIndex+1 {
		assert(false, fmt.Sprintf("log.append: mismatch %d, %d", e.index, s.lastLogIndex))
	}
	s.lastLogIndex, s.lastLogTerm = e.index, e.term
}

// never called with invalid index
func (s *storage) deleteLTE(meta SnapshotMeta) error {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	debug("deleteLTE meta.index:", meta.Index, "snapIndex:", s.snapIndex, "lastLogIndex:", s.lastLogIndex)
	n := meta.Index - s.snapIndex
	if err := s.log.DeleteFirst(n); err != nil {
		return opError(err, "Log.DeleteFirst(%d)", n)
	}
	s.snapIndex, s.snapTerm = meta.Index, meta.Term
	return nil
}

// called by raft.runLoop. no other calls made during this
// never called with invalid index
func (s *storage) deleteGTE(index, prevTerm uint64) error {
	n := s.lastLogIndex - index + 1
	if err := s.log.DeleteLast(n); err != nil {
		return opError(err, "Log.DeleteLast(%d)", n)
	}
	s.lastLogIndex, s.lastLogTerm = index-1, prevTerm
	return nil
}

func (s *storage) bootstrap(config Config) error {
	s.appendEntry(config.encode())
	s.setTerm(1)
	return nil
}
