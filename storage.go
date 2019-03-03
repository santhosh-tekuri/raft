package raft

import (
	"bytes"
	"fmt"
	"io"
)

// -------------------------------------------------------------

type Vars interface {
	GetVote() (term uint64, vote string, err error)
	SetVote(term uint64, vote string) error
}

type vars struct {
	storage  Vars
	term     uint64
	votedFor ID
}

func newVars(storage Vars) *vars {
	return &vars{storage: storage}
}

func (v *vars) init() error {
	term, vote, err := v.storage.GetVote()
	if err != nil {
		return fmt.Errorf("raft: Vars.GetVote failed: %v", err)
	}
	v.term, v.votedFor = term, ID(vote)
	return nil
}

func (v *vars) setTerm(term uint64) {
	if err := v.storage.SetVote(v.term, ""); err != nil {
		panic(fmt.Sprintf("raft: Vars.SetVote failed: %v", err))
	}
	v.term, v.votedFor = term, ""
}

func (v *vars) setVotedFor(id ID) {
	if err := v.storage.SetVote(v.term, string(id)); err != nil {
		panic(fmt.Sprintf("raft: Vars.SetVote failed: %v", err))
	}
	v.votedFor = id
}

// ---------------------------------------------------------

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

// todo: can we avoid panics on storage error

type log struct {
	storage  Log
	lastTerm uint64

	firstIndex uint64
	lastIndex  uint64
}

func newLog(storage Log) *log {
	return &log{storage: storage}
}

func (log *log) init() error {
	empty, err := log.storage.Empty()
	if err != nil {
		return fmt.Errorf("raft: Log.Empty failed: %v", err)

	}
	if empty {
		log.firstIndex, log.lastIndex = 1, 0
		log.lastTerm = 0
		return nil
	}

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
	if first, err = getEntry(log.storage.First); err != nil {
		return err
	}
	if last, err = getEntry(log.storage.Last); err != nil {
		return err
	}
	log.firstIndex = first.index
	log.lastIndex = last.index
	log.lastTerm = last.term

	return nil
}

func (log *log) count() uint64 {
	if log.firstIndex > log.lastIndex {
		return 0
	}
	return log.lastIndex - log.firstIndex + 1
}

// if empty returns nil
func (log *log) lastEntry() (*entry, error) {
	if log.count() == 0 {
		return nil, nil
	}
	b, err := log.storage.Last()
	if err != nil {
		return nil, err
	}
	e := &entry{}
	err = e.decode(bytes.NewReader(b))
	return e, err
}

// called by raft.runLoop and repl.runLoop. append call can be called during this
// never called with invalid index
func (log *log) getEntry(index uint64, e *entry) {
	log.validate(index)
	offset := index - log.firstIndex

	b, err := log.storage.Get(offset)
	if err != nil {
		panic(fmt.Sprintf("raft: log.get(%d) failed: %v", index, err))
	}
	if err = e.decode(bytes.NewReader(b)); err != nil {
		panic(fmt.Sprintf("raft: entry.decode(%d) failed: %v", index, err))
	}
}

// called by raft startup. no other calls made during this
// never called with invalid index
func (log *log) getConfig(index uint64) (Config, error) {
	e := &entry{}
	log.getEntry(index, e)
	config := Config{}
	err := config.decode(e)
	return config, err
}

// called by raft.runLoop. getEntry call can be called during this
func (log *log) append(e *entry) {
	w := new(bytes.Buffer)
	if err := e.encode(w); err != nil {
		panic(fmt.Sprintf("raft: entry.encode(%d) failed: %v", e.index, err))
	}
	if err := log.storage.Append(w.Bytes()); err != nil {
		panic(fmt.Sprintf("raft: log.append(%d): %v", e.index, err))
	}

	if e.index != log.lastIndex+1 {
		panic("log.append: entry.index!=lastIndex+1")
	}
	log.lastIndex, log.lastTerm = e.index, e.term
	log.lastTerm = e.term
}

// never called with invalid index
func (log *log) deleteLTE(index uint64) error {
	log.validate(index)
	n := index - log.firstIndex + 1
	if err := log.storage.DeleteFirst(n); err != nil {
		return fmt.Errorf("raft: log.deleteFirst(%d) failed: %v", n, err)
	}
	log.firstIndex = index + 1
	return nil
}

// called by raft.runLoop. no other calls made during this
// never called with invalid index
func (log *log) deleteGTE(index, prevTerm uint64) {
	log.validate(index)
	n := log.lastIndex - index + 1
	if err := log.storage.DeleteLast(n); err != nil {
		panic(fmt.Sprintf("raft: log.deleteLast(%d) failed: %v", n, err))
	}
	log.lastIndex = index - 1
	log.lastTerm = prevTerm
}

func (log *log) validate(index uint64) {
	//if log.count() == 0 {
	//	panic("raft: [BUG] index on empty log")
	//}
	//if index < log.firstIndex || index > log.lastIndex {
	//	panic(fmt.Sprintf("raft: [BUG] first %d, last %d, index %d: out of bounds", log.firstIndex, log.lastIndex, index))
	//}
}

// -----------------------------------------------------------------------------------

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

type storage struct {
	*vars
	log       *log
	snapIndex uint64
	snapTerm  uint64
	snapshots Snapshots
	configs   Configs
}

func newStorage(s Storage) *storage {
	return &storage{
		vars:      newVars(s.Vars),
		log:       newLog(s.Log),
		snapshots: s.Snapshots,
	}
}

func (s *storage) init() error {
	var err error
	if err = s.vars.init(); err != nil {
		return err
	}

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

	// init log ----------------
	if err = s.log.init(); err != nil {
		return err
	}
	if s.snapIndex > 0 {
		s.log.firstIndex, s.log.lastIndex = s.snapIndex+1, s.snapIndex
		s.log.lastTerm = s.snapTerm
	}

	// load configs ----------------
	need := 2
	for i := s.log.lastIndex; i >= s.log.firstIndex; i-- {
		e := &entry{}
		s.log.getEntry(i, e)
		if e.typ == entryConfig {
			var err error
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
	if need == 1 && s.snapIndex > 0 {
		s.configs.Committed = snapMeta.Config
	}

	return nil
}

func (s *storage) bootstrap(nodes map[ID]Node) (Config, error) {
	// wipe out if log is not empty
	if count := s.log.count(); count > 0 {
		if err := s.log.storage.DeleteFirst(count); err != nil {
			return Config{}, err
		}
	}

	config := Config{
		Nodes: nodes,
		Index: 1,
		Term:  1,
	}
	s.log.append(config.encode())

	s.vars.setTerm(1)
	return config, nil
}
