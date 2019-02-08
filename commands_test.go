package raft

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func TestCommands(t *testing.T) {
	type command interface {
		decode(r io.Reader) error
		encode(w io.Writer) error
	}
	tests := []command{
		&entry{index: 3, term: 5, typ: 2, data: []byte("sleep")},
		&requestVoteRequest{term: 5, candidateID: "localhost:1234", lastLogIndex: 3, lastLogTerm: 5},
		&requestVoteResponse{term: 5, voteGranted: true},
		&appendEntriesRequest{
			term: 5, leaderID: "localhost:5678", prevLogIndex: 3, prevLogTerm: 5,
			entries: []*entry{
				&entry{index: 3, term: 5, typ: 2, data: []byte("sleep")},
				&entry{index: 4, term: 5, typ: 3, data: []byte("wakeup")},
			}, leaderCommitIndex: 7,
		},
		&appendEntriesResponse{term: 5, success: true},
	}
	for _, test := range tests {
		name := fmt.Sprintf("command(%T)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			if err := test.encode(b); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			typ := reflect.TypeOf(test).Elem()
			cmd := reflect.New(typ).Interface().(command)
			if err := cmd.decode(b); err != nil {
				t.Fatalf("decode failed: %v", err)
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
