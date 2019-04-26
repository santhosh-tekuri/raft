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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/santhosh-tekuri/raft/log"
)

// SetIdentity stores the identity in storageDir.
// If identity is already set, it asserts that stored
// identity matches with given identity. It is recommended
// to call SetIdentity before using storageDir.
//
// If the storageDir is already in use, returns ErrLockExists.
// If the stored identity does not match given identity, returns ErrIdentityAlreadySet.
func SetIdentity(storageDir string, cid, nid uint64) (err error) {
	if cid == 0 {
		return errors.New("raft: cid is zero")
	}
	if nid == 0 {
		return errors.New("raft: nid is zero")
	}
	d, err := os.Stat(storageDir)
	if err != nil {
		return err
	}
	if !d.IsDir() {
		return fmt.Errorf("raft: %q is not a diretory", storageDir)
	}
	if err := lockDir(storageDir); err != nil {
		return err
	}
	defer func() {
		err = unlockDir(storageDir)
	}()
	val, err := openValue(storageDir, ".id")
	if err != nil {
		return err
	}
	if cid == val.v1 && nid == val.v2 {
		return nil
	}
	if val.v1 != 0 && val.v2 != 0 {
		return ErrIdentityAlreadySet
	}
	return val.set(cid, nid)
}

type storage struct {
	idVal *value
	cid   uint64
	nid   uint64

	termVal  *value
	term     uint64
	votedFor uint64

	log          *log.Log
	lastLogIndex uint64
	lastLogTerm  uint64

	snaps   *snapshots
	configs Configs
}

func openStorage(dir string, opt Options) (*storage, error) {
	s, err := &storage{}, error(nil)
	defer func() {
		if err != nil {
			if s.log != nil {
				_ = s.log.Close()
			}
		}
	}()

	// open identity value ----------------
	if s.idVal, err = openValue(dir, ".id"); err != nil {
		return nil, err
	}
	s.cid, s.nid = s.idVal.get()

	// open term value ----------------
	if s.termVal, err = openValue(dir, ".term"); err != nil {
		return nil, err
	}
	s.term, s.votedFor = s.termVal.get()

	// open snapshots ----------------
	if s.snaps, err = openSnapshots(filepath.Join(dir, "snapshots"), opt); err != nil {
		return nil, err
	}
	s.lastLogIndex, s.lastLogTerm = s.snaps.index, s.snaps.term
	meta, err := s.snaps.meta()
	if err != nil {
		return nil, err
	}

	// open log ----------------
	logOpt := log.Options{
		FileMode:    0600,
		SegmentSize: opt.LogSegmentSize,
	}
	if s.log, err = log.Open(filepath.Join(dir, "log"), 0700, logOpt); err != nil {
		return nil, err
	}
	if s.log.Count() > 0 {
		data, err := s.log.Get(s.log.LastIndex())
		if err != nil {
			return nil, opError(err, "Log.Get(%d)", s.log.LastIndex())
		}
		e := &entry{}
		if err := e.decode(bytes.NewReader(data)); err != nil {
			return nil, opError(err, "Log.Get(%d).decode", s.log.LastIndex())
		}
		assert(e.index == s.log.LastIndex())
		s.lastLogIndex, s.lastLogTerm = e.index, e.term
	}

	// load configs ----------------
	need := 2
	for i := s.lastLogIndex; i > s.snaps.index; i-- {
		e := &entry{}
		if err = s.getEntry(i, e); err != nil {
			return nil, err
		}
		if e.typ == entryConfig {
			if need == 2 {
				err = s.configs.Latest.decode(e)
			} else {
				err = s.configs.Committed.decode(e)
			}
			if err != nil {
				return nil, err
			}
			need--
			if need == 0 {
				break
			}
		}
	}
	if need == 2 {
		s.configs.Latest = meta.config
		need--
	}
	if need == 1 {
		s.configs.Committed = meta.config
	}

	return s, nil
}

func (s *storage) setTerm(term uint64) {
	if s.term != term {
		assert(term > s.term)
		if err := s.termVal.set(term, 0); err != nil {
			panic(opError(err, "storage.setTermVote(%d, %d)", term, 0))
		}
		s.term, s.votedFor = term, 0
	}
}

var grantingVote = func(s *storage, term, candidate uint64) error { return nil }

func (s *storage) setVotedFor(term, candidate uint64) {
	if term != s.term || candidate != s.votedFor {
		assert(term >= s.term)
		err := grantingVote(s, term, candidate)
		if err == nil {
			err = s.termVal.set(term, candidate)
		}
		if err != nil {
			panic(opError(err, "storage.setTermVote(%d, %d)", term, candidate))
		}
		s.term, s.votedFor = term, candidate
	}
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
	b, err := s.log.Get(index)
	if err == log.ErrNotFound {
		return err
	} else if err != nil {
		panic(opError(err, "Log.Get(%d)", index))
	}
	if err = e.decode(bytes.NewReader(b)); err != nil {
		panic(opError(err, "log.Get(%d).decode()", index))
	}
	if e.index != index {
		panic(opError(fmt.Errorf("got %d, want %d", e.index, index), "log.Get(%d).index: ", index))
	}
	return nil
}

func (s *storage) mustGetEntry(index uint64, e *entry) {
	if err := s.getEntry(index, e); err != nil {
		panic(bug{fmt.Sprintf("storage.MustGetEntry(%d)", index), err})
	}
}

// called by raft.runLoop. getEntry call can be called during this
func (s *storage) appendEntry(e *entry) {
	assert(e.index == s.lastLogIndex+1)
	w := new(bytes.Buffer)
	if err := e.encode(w); err != nil {
		panic(bug{fmt.Sprintf("entry.encode(%d)", e.index), err})
	}
	if err := s.log.Append(w.Bytes()); err != nil {
		panic(opError(err, "Log.Append"))
	}
	s.lastLogIndex, s.lastLogTerm = e.index, e.term
}

func (s *storage) commitLog(n uint64) {
	if err := s.log.CommitN(n); err != nil {
		panic(opError(err, "Log.CommitN(%d)", n))
	}
}

// never called with invalid index
func (s *storage) removeLTE(index uint64) error {
	// todo: trace log compaction
	if err := s.log.RemoveLTE(index); err != nil {
		return opError(err, "Log.RemoveLTE(%d)", index)
	}
	return nil
}

func (r *Raft) compactLog(lte uint64) error {
	if trace {
		println(r, "compactLog", lte)
	}
	if err := r.storage.removeLTE(lte); err != nil {
		r.logger.Warn(trimPrefix(err))
		r.alerts.Error(err)
		return err
	}
	r.logger.Info("log upto index ", r.log.PrevIndex(), "is discarded")
	if tracer.logCompacted != nil {
		tracer.logCompacted(r)
	}
	return nil
}

// no replication is going on when this called
// todo: are you sure about this ???
func (s *storage) clearLog() error {
	if err := s.log.Reset(s.snaps.index); err != nil {
		return opError(err, "Log.Reset(%d)", s.snaps.index)
	}
	assert(s.log.LastIndex() == s.snaps.index)
	assert(s.log.PrevIndex() == s.snaps.index)
	s.lastLogIndex, s.lastLogTerm = s.snaps.index, s.snaps.term
	return nil
}

// called by raft.runLoop. no other calls made during this
// never called with invalid index
func (s *storage) removeGTE(index, prevTerm uint64) {
	if err := s.log.RemoveGTE(index); err != nil {
		panic(opError(err, "Log.RemoveGTE(%d)", index))
	}
	assert(s.log.LastIndex() == index-1)
	s.lastLogIndex, s.lastLogTerm = index-1, prevTerm
}

func (s *storage) bootstrap(config Config) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = recoverErr(v)
		}
	}()
	s.appendEntry(config.encode())
	s.commitLog(1)
	s.setTerm(1)
	s.lastLogIndex, s.lastLogTerm = config.Index, config.Term
	return nil
}
