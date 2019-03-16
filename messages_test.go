package raft

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestMessages(t *testing.T) {
	type message interface {
		decode(r io.Reader) error
		encode(w io.Writer) error
	}

	nodes := make(map[uint64]Node)
	nodes[1] = Node{ID: 1, Addr: "localhost:7000", Voter: true}
	nodes[2] = Node{ID: 2, Addr: "localhost:8000", Voter: false}
	nodes[3] = Node{ID: 3, Addr: "localhost:9000", Promote: true}

	snapshot := "helloworld"
	tests := []message{
		&entry{index: 3, term: 5, typ: 2, data: []byte("sleep")},
		&voteReq{term: 5, candidate: 2, lastLogIndex: 3, lastLogTerm: 5},
		&voteResp{resp{term: 5, result: success}},
		&voteResp{resp{term: 5, result: alreadyVoted}},
		&appendEntriesReq{
			term: 5, leader: 2, prevLogIndex: 3, prevLogTerm: 5,
			entries: []*entry{
				{index: 3, term: 5, typ: 2, data: []byte("sleep")},
				{index: 4, term: 5, typ: 3, data: []byte("wakeup")},
			}, ldrCommitIndex: 7,
		},
		&appendEntriesResp{resp: resp{term: 5, result: success}, lastLogIndex: 9},
		&installSnapReq{
			term: 5, leader: 1, lastIndex: 3, lastTerm: 5,
			lastConfig: Config{
				Nodes: nodes,
				Index: 1, Term: 2,
			}, size: int64(len(snapshot)),
			snapshot: ioutil.NopCloser(bytes.NewReader([]byte(snapshot))),
		},
		&installSnapResp{resp{term: 5, result: success}},
		&installSnapResp{resp{term: 5, result: unexpectedErr}},
		&timeoutNowReq{term: 5, leader: 3},
		&timeoutNowResp{resp{term: 5, result: success}},
	}
	for _, test := range tests {
		name := fmt.Sprintf("message(%T)", test)
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

			if test, ok := test.(*installSnapReq); ok {
				test.snapshot = nil
				cmd := cmd.(*installSnapReq)
				cmdSnapshot, err := ioutil.ReadAll(cmd.snapshot)
				if err != nil {
					t.Fatalf("snapshot read failed: %v", err)
				}
				if string(cmdSnapshot) != snapshot {
					t.Fatalf("snapshot: got %s, want %s", cmdSnapshot, snapshot)
				}
				cmd.snapshot = nil
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
