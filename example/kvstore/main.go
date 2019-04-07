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
	"net"
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
	opt := raft.DefaultOptions()
	storeOpt := raft.DefaultStorageOptions()
	store, err := raft.OpenStorage(os.Args[1], storeOpt)
	if err != nil {
		panic(err)
	}
	if cid, nid := store.GetIdentity(); cid == 0 && nid == 0 {
		cid, nid = lookupUint64("CID"), lookupUint64("NID")
		if err := store.SetIdentity(cid, nid); err != nil {
			panic(err)
		}
	}
	s := newKVStore()
	r, err := raft.New(opt, s, store)
	if err != nil {
		panic(err)
	}

	lr, err := net.Listen("tcp", os.Args[2])
	if err != nil {
		panic(err)
	}
	go http.ListenAndServe(os.Args[3], handler{r})
	err = r.Serve(lr)
	if err != raft.ErrServerClosed {
		panic(err)
	}
}

func lookupUint64(key string) uint64 {
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
