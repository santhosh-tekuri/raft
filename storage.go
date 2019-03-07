package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

type Vars interface {
	GetVote() (term uint64, vote string, err error)
	SetVote(term uint64, vote string) error
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

var errNoEntryFound = errors.New("raft: no entry found")

// todo: can we avoid panics on storage error
type storage struct {
	vars     Vars
	term     uint64
	votedFor ID

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
	term, vote, err := s.vars.GetVote()
	if err != nil {
		return fmt.Errorf("raft: Vars.GetVote failed: %v", err)
	}
	s.term, s.votedFor = term, ID(vote)

	// init snapshots ---------------
	meta, err := s.snapshots.Meta()
	if err != nil {
		return err
	}
	s.snapIndex, s.snapTerm = meta.Index, meta.Term

	// init log ---------------------
	count, err := s.log.Count()
	if err != nil {
		return err
	}
	s.lastLogIndex = s.snapIndex + count
	if count == 0 {
		s.lastLogTerm = s.snapTerm
	} else {
		s.lastLogTerm, _ = s.getEntryTerm(s.lastLogIndex)
	}

	// load configs ----------------
	need := 2
	for i := s.lastLogIndex; i > s.snapIndex; i-- {
		e := &entry{}
		_ = s.getEntry(i, e)
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
	if err := s.vars.SetVote(s.term, ""); err != nil {
		panic(fmt.Sprintf("raft: Vars.SetVote failed: %v", err))
	}
	s.term, s.votedFor = term, ""
}

func (s *storage) setVotedFor(id ID) {
	if err := s.vars.SetVote(s.term, string(id)); err != nil {
		panic(fmt.Sprintf("raft: Vars.SetVote failed: %v", err))
	}
	s.votedFor = id
}

// NOTE: this should not be called with snapIndex
func (s *storage) getEntryTerm(index uint64) (uint64, error) {
	e := &entry{}
	err := s.getEntry(index, e)
	return e.term, err
}

// called by raft.runLoop and repl.runLoop. append call can be called during this
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
		panic(fmt.Sprintf("raft: log.get(%d) failed: %v", index, err))
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
		panic(fmt.Sprintf("raft: log.append(%d): %v", e.index, err))
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
	debug("deleteLTE", meta.Index, s.snapIndex, s.lastLogIndex)
	n := meta.Index - s.snapIndex
	if err := s.log.DeleteFirst(n); err != nil {
		return fmt.Errorf("raft: log.deleteFirst(%d) failed: %v", n, err)
	}
	s.snapIndex, s.snapTerm = meta.Index, meta.Term
	return nil
}

// called by raft.runLoop. no other calls made during this
// never called with invalid index
func (s *storage) deleteGTE(index, prevTerm uint64) {
	n := s.lastLogIndex - index + 1
	if err := s.log.DeleteLast(n); err != nil {
		panic(fmt.Sprintf("raft: log.deleteLast(%d) failed: %v", n, err))
	}
	s.lastLogIndex, s.lastLogTerm = index-1, prevTerm
}

func (s *storage) bootstrap(config Config) error {
	s.appendEntry(config.encode())
	s.setTerm(1)
	return nil
}
