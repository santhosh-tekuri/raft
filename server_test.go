package raft

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/santhosh-tekuri/fnet"
)

func TestServer(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name string
		req  request
		resp response
	}{
		{
			name: "requestVote",
			req:  &voteReq{req: req{term: 5, src: 2}, lastLogIndex: 3, lastLogTerm: 5},
			resp: &voteResp{resp{term: 5, result: success}},
		},
		{
			name: "appendEntries",
			req: &appendEntriesReq{
				req: req{term: 5, src: 3}, prevLogIndex: 3, prevLogTerm: 5,
				numEntries: 7, ldrCommitIndex: 7,
			},
			resp: &appendEntriesResp{resp: resp{term: 5, result: success}, lastLogIndex: 10},
		},
	}

	nw := fnet.New()
	earth, addr := nw.Host("earth"), "earth:8888"

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lr, err := earth.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("server.listen failed: %v", err)
			}
			s := newServer(lr)

			rpcCh := make(chan *rpc)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.serve(rpcCh)
			}()
			defer s.shutdown()

			go func() {
				for rpc := range rpcCh {
					if !reflect.DeepEqual(rpc.req, test.req) {
						t.Errorf("request mismatch: got %#v, want %#v", rpc.req, test.req)
					}
					rpc.resp = test.resp
					close(rpc.done)
				}
			}()

			c, err := dial(earth.DialTimeout, addr, 10*time.Second)
			if err != nil {
				t.Fatalf("dial failed: %v", err)
			}
			defer c.rwc.Close()
			resp := reflect.New(reflect.TypeOf(test.resp).Elem()).Interface().(response)
			if err := c.doRPC(test.req, resp); err != nil {
				t.Fatalf("c.doRPC() failed: %v", err)
			}
			if !reflect.DeepEqual(resp, test.resp) {
				t.Fatalf("response mismatch: got %#v, want %#v", resp, test.resp)
			}
		})
	}
}
