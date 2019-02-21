package raft

import (
	"reflect"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/santhosh-tekuri/fnet"
)

func TestServer(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name      string
		typ       rpcType
		req, resp message
	}{
		{
			name: "requestVote",
			typ:  rpcVote,
			req:  &voteRequest{term: 5, candidateID: "localhost:1234", lastLogIndex: 3, lastLogTerm: 5},
			resp: &voteResponse{term: 5, granted: true},
		},
		{
			name: "appendEntries",
			typ:  rpcAppendEntries,
			req: &appendEntriesRequest{
				term: 5, leaderID: "localhost:5678", prevLogIndex: 3, prevLogTerm: 5,
				entries: []*entry{
					{index: 3, term: 5, typ: 2, data: []byte("sleep")},
					{index: 4, term: 5, typ: 3, data: []byte("wakeup")},
				}, leaderCommitIndex: 7,
			},
			resp: &appendEntriesResponse{term: 5, success: true},
		},
	}

	nw := fnet.New()
	earth, addr := nw.Host("earth"), "earth:8888"

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &server{listenFn: earth.Listen}
			if err := s.listen(addr); err != nil {
				t.Fatalf("server.listen failed: %v", err)
			}
			go s.serve()
			defer s.shutdown()

			go func() {
				for rpc := range s.rpcCh {
					if !reflect.DeepEqual(rpc.req, test.req) {
						t.Errorf("request mismatch: got %#v, want %#v", rpc.req, test.req)
					}
					rpc.respCh <- test.resp
				}
			}()

			c, err := dial(earth.DialTimeout, addr, 10*time.Second)
			if err != nil {
				t.Fatalf("dial failed: %v", err)
			}
			defer c.close()
			resp := reflect.New(reflect.TypeOf(test.resp).Elem()).Interface().(message)
			if err := c.doRPC(test.typ, test.req, resp); err != nil {
				t.Fatalf("c.doRPC() failed: %v", err)
			}
			if !reflect.DeepEqual(resp, test.resp) {
				t.Fatalf("response mismatch: got %#v, want %#v", resp, test.resp)
			}
		})
	}
}
