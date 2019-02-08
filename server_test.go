package raft

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"
)

func freeAddrs(n int) []string {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}

	var free []string
	for i := 0; i < n; i++ {
		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			panic(err)
		}
		defer l.Close()
		free = append(free, fmt.Sprintf("localhost:%d", l.Addr().(*net.TCPAddr).Port))
	}
	return free
}

func TestServer(t *testing.T) {
	addr := freeAddrs(1)[0]

	tests := []struct {
		name      string
		typ       rpcType
		req, resp command
	}{
		{
			name: "requestVote",
			typ:  rpcRequestVote,
			req:  &requestVoteRequest{term: 5, candidateID: "localhost:1234", lastLogIndex: 3, lastLogTerm: 5},
			resp: &requestVoteResponse{term: 5, voteGranted: true},
		},
		{
			name: "appendEntries",
			typ:  rpcAppendEntries,
			req: &appendEntriesRequest{
				term: 5, leaderID: "localhost:5678", prevLogIndex: 3, prevLogTerm: 5,
				entries: []*entry{
					&entry{index: 3, term: 5, typ: 2, data: []byte("sleep")},
					&entry{index: 4, term: 5, typ: 3, data: []byte("wakeup")},
				}, leaderCommitIndex: 7,
			},
			resp: &appendEntriesResponse{term: 5, success: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := startServer(addr)
			if err != nil {
				t.Fatalf("startServer failed: %v", err)
			}
			defer s.shutdown()

			go func() {
				for rpc := range s.rpcCh {
					if !reflect.DeepEqual(rpc.req, test.req) {
						t.Errorf("request mismatch: got %#v, want %#v", rpc.req, test.req)
					}
					rpc.respCh <- test.resp
				}
			}()

			c, err := dial(addr, 10*time.Second)
			if err != nil {
				t.Fatalf("dial failed: %v", err)
			}
			defer c.close()
			resp := reflect.New(reflect.TypeOf(test.resp).Elem()).Interface().(command)
			if err := c.doRPC(test.typ, test.req, resp); err != nil {
				t.Fatalf("client.do() failed: %v", err)
			}
			if !reflect.DeepEqual(resp, test.resp) {
				t.Fatalf("response mismatch: got %#v, want %#v", resp, test.resp)
			}
		})
	}
}
