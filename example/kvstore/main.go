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
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/santhosh-tekuri/raft"
)

func main() {
	if len(os.Args) != 4 {
		errln("usage: raft <storage-dir> <raft-addr> <http-addr>")
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
