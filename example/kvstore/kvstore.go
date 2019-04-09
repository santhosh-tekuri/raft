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

package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	"github.com/santhosh-tekuri/raft"
)

type kvStore struct {
	data map[string]string
}

func newKVStore() *kvStore {
	return &kvStore{make(map[string]string)}
}

func (s *kvStore) Update(b []byte) interface{} {
	cmd, err := decodeCmd(b)
	if err != nil {
		return err
	}
	switch cmd := cmd.(type) {
	case set:
		s.data[cmd.Key] = cmd.Val
		return nil
	case del:
		delete(s.data, cmd.Key)
		return nil
	}
	return fmt.Errorf("unknown cmd: %T", cmd)
}

func (s *kvStore) Read(cmd interface{}) interface{} {
	switch cmd := cmd.(type) {
	case get:
		return s.data[cmd.Key]
	}
	return fmt.Errorf("unknown cmd: %T", cmd)
}

func (s *kvStore) Snapshot() (raft.FSMState, error) {
	data := make(map[string]string)
	for k, v := range s.data {
		data[k] = v
	}
	return &kvState{data}, nil
}

func (s *kvStore) RestoreFrom(r io.Reader) error {
	var data map[string]string
	if err := gob.NewDecoder(r).Decode(&data); err != nil {
		return err
	}
	s.data = data
	return nil
}

// commands --------------------------------------------------

type cmdType byte

const (
	cmdSet cmdType = iota
	cmdDel
)

type set struct {
	Key, Val string
}

type get struct {
	Key string
}

type del struct {
	Key string
}

func decodeCmd(b []byte) (interface{}, error) {
	if len(b) == 0 {
		return nil, errors.New("no data")
	}
	decoder := gob.NewDecoder(bytes.NewReader(b[1:]))
	switch cmdType(b[0]) {
	case cmdSet:
		cmd := set{}
		if err := decoder.Decode(&cmd); err != nil {
			return nil, err
		}
		return cmd, nil
	case cmdDel:
		cmd := del{}
		if err := decoder.Decode(&cmd); err != nil {
			return nil, err
		}
		return cmd, nil
	default:
		return nil, fmt.Errorf("unknown cmd: %d", b[0])
	}
}

func encodeCmd(cmd interface{}) []byte {
	var typ cmdType
	switch cmd.(type) {
	case set:
		typ = cmdSet
	case del:
		typ = cmdDel
	default:
		panic(fmt.Errorf("encodeCmd: %T", cmd))
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(typ))
	if err := gob.NewEncoder(buf).Encode(cmd); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// state --------------------------------------------------

type kvState struct {
	data map[string]string
}

func (s *kvState) WriteTo(w io.Writer) error {
	return gob.NewEncoder(w).Encode(s.data)
}

func (s *kvState) Release() {}
