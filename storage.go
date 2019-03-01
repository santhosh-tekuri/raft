package raft

import (
	"bytes"
	"fmt"
)

// -------------------------------------------------------------

type Vars interface {
	GetVote() (term uint64, vote string, err error)
	SetVote(term uint64, vote string) error
	GetConfig() (committed, latest uint64, err error)
	SetConfig(committed, latest uint64) error
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
func (log *log) deleteLTE(index uint64) {
	if log.count() == 0 {
		panic("raft: [BUG] Storage.deleteLTE on empty log")
	}
	if index < log.firstIndex || index > log.lastIndex {
		panic(fmt.Sprintf("raft: [BUG] log.deleteLTE(%d) failed: out of bounds", index))
	}
	n := index - log.firstIndex + 1
	if err := log.storage.DeleteFirst(n); err != nil {
		panic(fmt.Sprintf("raft: log.deleteFirst(%d) failed: %v", n, err))
	}
	log.firstIndex = index + 1
}

// called by raft.runLoop. no other calls made during this
// never called with invalid index
func (log *log) deleteGTE(index uint64) {
	n := log.lastIndex - index + 1
	if err := log.storage.DeleteLast(n); err != nil {
		panic(fmt.Sprintf("raft: log.deleteLast(%d) failed: %v", n, err))
	}
	log.lastIndex = index - 1 // lasTerm is updated on immediate append call
}

// -----------------------------------------------------------------------------------

type Storage struct {
	Vars Vars
	Log  Log
}

type storage struct {
	*vars
	log     *log
	configs Configs
}

func newStorage(s Storage) *storage {
	return &storage{
		vars: newVars(s.Vars),
		log:  newLog(s.Log),
	}
}

func (s *storage) init() error {
	if err := s.vars.init(); err != nil {
		return err
	}
	if err := s.log.init(); err != nil {
		return err
	}

	// load configs
	committed, latest, err := s.vars.storage.GetConfig()
	if err != nil {
		return err
	}
	if committed != 0 {
		s.configs.Committed, err = s.log.getConfig(committed)
		if err != nil {
			return err
		}
	}
	if latest != 0 {
		s.configs.Latest, err = s.log.getConfig(latest)
		if err != nil {
			return err
		}
	}

	// handle the case, where config is stored in log but
	// crashed before saving in vars
	last, err := s.log.lastEntry()
	if err != nil {
		return err
	}
	if last != nil && last.typ == entryConfig && last.index > latest {
		if err := s.configs.Latest.decode(last); err != nil {
			return err
		}
	}

	return nil
}

func (s *storage) saveConfigs() {
	if err := s.vars.storage.SetConfig(s.configs.Committed.Index, s.configs.Latest.Index); err != nil {
		panic(fmt.Sprintf("raft: Vars.SetConfigs failed: %v", err))
	}
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
	if err := s.vars.storage.SetConfig(0, 1); err != nil {
		return config, err
	}
	return config, nil
}
