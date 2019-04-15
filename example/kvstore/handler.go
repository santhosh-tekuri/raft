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
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/santhosh-tekuri/raft"
)

type handler struct {
	r *raft.Raft
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 2 || parts[1] == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	key := parts[1]
	switch r.Method {
	case http.MethodGet:
		res, err := h.execute(raft.ReadFSM(get{key}))
		if err != nil {
			h.replyErr(w, r, err)
		} else {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(res.(string)))
		}
	case http.MethodPost:
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, err = h.execute(raft.UpdateFSM(encodeCmd(set{key, string(b)})))
		if err != nil {
			h.replyErr(w, r, err)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	case http.MethodDelete:
		_, err := h.execute(raft.UpdateFSM(encodeCmd(del{key})))
		if err != nil {
			h.replyErr(w, r, err)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h handler) replyErr(w http.ResponseWriter, r *http.Request, err error) {
	if err, ok := err.(raft.NotLeaderError); ok && err.Leader.ID != 0 {
		url := fmt.Sprintf("http://%s%s", err.Leader.Data, r.URL.Path)
		http.Redirect(w, r, url, http.StatusPermanentRedirect)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func (h handler) execute(t raft.FSMTask) (interface{}, error) {
	select {
	case <-h.r.Closed():
		return nil, raft.ErrServerClosed
	case h.r.FSMTasks() <- t:
	}
	<-t.Done()
	return t.Result(), t.Err()
}
