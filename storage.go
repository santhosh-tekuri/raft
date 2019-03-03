package raft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

type Vars interface {
	GetVote() (term uint64, vote string, err error)
	SetVote(term uint64, vote string) error
}

type Log interface {
	Empty() (bool, error)

	// First returns first entry.
	// This is never called on empty log.
	First() ([]byte, error)

	// Last returns last entry.
	// This is never called on empty log.
	Last() ([]byte, error)

	Get(offset uint64) ([]byte, error)
	Append(entry []byte) error
	DeleteFirst(n uint64) error
	DeleteLast(n uint64) error
}

type Snapshots interface {
	New(index, term uint64, config Config) (SnapshotSink, error)
	List() ([]uint64, error)
	Meta(index uint64) (SnapshotMeta, error)
	Open(index uint64) (io.ReadCloser, error)
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
	votedFor ID

	log             Log
	firstLogIndexMu sync.RWMutex // todo
	firstLogIndex   uint64
	lastLogIndex    uint64
	lastLogTerm     uint64

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
	snaps, err := s.snapshots.List()
	if err != nil {
		return err
	}
	var snapMeta SnapshotMeta
	if len(snaps) > 0 {
		snapMeta, err = s.snapshots.Meta(snaps[0])
		if err != nil {
			return err
		}
		s.snapIndex, s.snapTerm = snapMeta.Index, snapMeta.Term
	}

	// init log ---------------------
	empty, err := s.log.Empty()
	if err != nil {
		return fmt.Errorf("raft: Log.Empty failed: %v", err)
	}
	if empty {
		if s.snapIndex > 0 {
			s.firstLogIndex, s.lastLogIndex, s.lastLogTerm = s.snapIndex+1, s.snapIndex, s.snapTerm
		} else {
			s.firstLogIndex, s.lastLogIndex, s.lastLogTerm = 1, 0, 0
		}
	} else {
		getEntry := func(get func() ([]byte, error)) (*entry, error) {
			b, err := get()
			if err != nil {
				return nil, fmt.Errorf("raft: Log.GetFirst/GetLast failed: %v", err)
			}
			e := &entry{}
			if err = e.decode(bytes.NewReader(b)); err != nil {
				return nil, err
			}
			return e, nil
		}
		var first, last *entry
		if first, err = getEntry(s.log.First); err != nil {
			return err
		}
		if last, err = getEntry(s.log.Last); err != nil {
			return err
		}
		s.firstLogIndex, s.lastLogIndex, s.lastLogTerm = first.index, last.index, last.term
	}

	// load configs ----------------
	need := 2
	for i := s.lastLogIndex; i >= s.firstLogIndex; i-- {
		e := &entry{}
		s.getEntry(i, e)
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
	if s.snapIndex > 0 {
		if need == 2 {
			s.configs.Latest = snapMeta.Config
			need--
		}
		if need == 1 {
			s.configs.Committed = snapMeta.Config
		}
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

func (s *storage) entryCount() uint64 {
	if s.firstLogIndex > s.lastLogIndex {
		return 0
	}
	return s.lastLogIndex - s.firstLogIndex + 1
}

func (s *storage) getEntryTerm(index uint64) uint64 {
	e := &entry{}
	s.getEntry(index, e)
	return e.term
}

// called by raft.runLoop and repl.runLoop. append call can be called during this
// never called with invalid index
func (s *storage) getEntry(index uint64, e *entry) {
	offset := index - s.firstLogIndex
	b, err := s.log.Get(offset)
	if err != nil {
		panic(fmt.Sprintf("raft: log.get(%d) failed: %v", index, err))
	}
	if err = e.decode(bytes.NewReader(b)); err != nil {
		panic(fmt.Sprintf("raft: entry.decode(%d) failed: %v", index, err))
	}
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
		panic("log.append: entry.index!=lastIndex+1")
	}
	s.lastLogIndex, s.lastLogTerm = e.index, e.term
}

// never called with invalid index
func (s *storage) deleteLTE(index uint64) error {
	n := index - s.firstLogIndex + 1
	if err := s.log.DeleteFirst(n); err != nil {
		return fmt.Errorf("raft: log.deleteFirst(%d) failed: %v", n, err)
	}
	s.firstLogIndex = index + 1
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

func (s *storage) bootstrap(nodes map[ID]Node) (Config, error) {
	config := Config{
		Nodes: nodes,
		Index: 1,
		Term:  1,
	}
	s.appendEntry(config.encode())
	s.setTerm(1)
	return config, nil
}
