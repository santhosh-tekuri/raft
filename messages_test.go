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
	"io"
	"reflect"
	"testing"
)

func TestMessage(t *testing.T) {
	type message interface {
		decode(r io.Reader) error
		encode(w io.Writer) error
	}

	nodes := make(map[uint64]Node)
	nodes[1] = Node{ID: 1, Addr: "localhost:7000", Voter: true}
	nodes[2] = Node{ID: 2, Addr: "localhost:8000", Voter: false}
	nodes[3] = Node{ID: 3, Addr: "localhost:9000", Action: Promote}

	snapshot := "helloworld"
	tests := []message{
		&entry{index: 3, term: 5, typ: 2, data: []byte("sleep")},
		&voteReq{req: req{term: 5, src: 2}, lastLogIndex: 3, lastLogTerm: 5, transfer: true},
		&voteResp{resp{term: 5, result: success}},
		&voteResp{resp{term: 5, result: alreadyVoted}},
		&appendEntriesReq{
			req: req{term: 5, src: 2}, prevLogIndex: 3, prevLogTerm: 5, numEntries: 10, ldrCommitIndex: 56,
		},
		&appendEntriesResp{resp: resp{term: 5, result: success}, lastLogIndex: 9},
		&installSnapReq{
			req: req{term: 5, src: 1}, lastIndex: 3, lastTerm: 5,
			lastConfig: Config{
				Nodes: nodes,
				Index: 1, Term: 2,
			}, size: int64(len(snapshot)),
		},
		&installSnapResp{resp{term: 5, result: success}},
		&installSnapResp{resp{term: 5, result: unexpectedErr, err: errors.New("notOpErr")}},
		&installSnapResp{resp{term: 5, result: unexpectedErr, err: OpError{"myop", errors.New("notOpErr")}}},
		&timeoutNowReq{req{term: 5, src: 3}},
		&timeoutNowResp{resp{term: 5, result: success}},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%T", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			if err := test.encode(b); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			typ := reflect.TypeOf(test).Elem()
			cmd := reflect.New(typ).Interface().(message)
			if err := cmd.decode(b); err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			// check resp.error
			if _, ok := test.(response); ok {
				testErr := test.(response).getErr()
				cmdErr := test.(response).getErr()
				test.(response).setErr(nil)
				cmd.(response).setErr(nil)
				if testOpErr, ok := testErr.(OpError); ok {
					cmdOperr, ok := cmdErr.(OpError)
					if !ok {
						t.Fatal("expected OpError")
					}
					if testOpErr.Op != cmdOperr.Op {
						t.Fatalf("opMismatch: %q, %q", testOpErr.Op, cmdOperr.Op)
					}
					testErr, cmdErr = testOpErr.Err, cmdOperr.Err
				}
				if testErr != nil {
					if cmdErr == nil {
						t.Fatal("expected error")
					}
					if testErr.Error() != cmdErr.Error() {
						t.Fatalf("errMismsatch: %q, %q", testErr.Error(), cmdErr.Error())
					}
				} else if cmdErr != nil {
					t.Fatal("expected nil error")
				}
			}

			if !reflect.DeepEqual(cmd, test) {
				t.Fatalf("mismatch: got %#v, want %#v", cmd, test)
			}
			if b.Len() != 0 {
				t.Fatalf("bytes left. got %d, want %d", b.Len(), 0)
			}
		})
	}
}
