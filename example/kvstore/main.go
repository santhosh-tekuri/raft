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

// Package kvstore demonstrates how to use raft package to implement simple kvstore.
//
//   usage: kvstore <storage-dir> <raft-addr> <http-addr>
//
//   storage-dir: where raft stores its data. Created if the directory does not exist
//   raft-addr:   raft server listens on this address
//   http-addr:   http server listens on this address
//
// in addition to this you should pass cluster-id and node-id using environment
// variables CID, NID respectively. value is uint64 greater than zero.
//
// Launch 3 nodes as shown below:
//   $ CID=1234 NID=1 kvstore data1 localhost:7001 localhost:8001
//   $ CID=1234 NID=2 kvstore data2 localhost:7002 localhost:8002
//   $ CID=1234 NID=3 kvstore data3 localhost:7003 localhost:8003
//
// Note: we are using Node.Data to store http-addr, which enables us to enable http redirects.
//
// to bootstrap cluster:
//   $ RAFT_ADDR=localhost:7001 raftctl config apply \
//        +nid=1,voter=true,addr=localhost:7001,data=localhost:8001 \
//        +nid=2,voter=true,addr=localhost:7002,data=localhost:8002 \
//        +nid=3,voter=true,addr=localhost:7003,data=localhost:8003
//   $ RAFT_ADDR=localhost:7001 raftctl config get
//
// to test kvstore:
//   $ curl -v -X POST localhost:8001/k1 -d v1
//   $ curl -v localhost:8001/k1
//   $ curl -v -L localhost:8002/k1
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/santhosh-tekuri/raft"
)

func main() {
	if len(os.Args) != 4 {
		errln("usage: kvstore <storage-dir> <raft-addr> <http-addr>")
		os.Exit(1)
	}
	storageDir, raftAddr, httpAddr := os.Args[1], os.Args[2], os.Args[3]
	if err := os.MkdirAll(storageDir, 0700); err != nil {
		panic(err)
	}
	cid, nid := lookupEnv("CID"), lookupEnv("NID")
	if err := raft.SetIdentity(storageDir, cid, nid); err != nil {
		panic(err)
	}

	store := newKVStore()
	opt := raft.DefaultOptions()
	r, err := raft.New(opt, store, storageDir)
	if err != nil {
		panic(err)
	}
	go http.ListenAndServe(httpAddr, handler{r})

	// always shutdown raft, otherwise lock file remains in storageDir
	go func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		_ = r.Shutdown(context.Background())
	}()

	err = r.ListenAndServe(raftAddr)
	if err != raft.ErrServerClosed && err != raft.ErrNodeRemoved {
		panic(err)
	}
}

func lookupEnv(key string) uint64 {
	s, ok := os.LookupEnv(key)
	if !ok {
		errln("environment variable", key, "not set")
		os.Exit(1)
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return uint64(i)
}

func errln(v ...interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, v...)
}
