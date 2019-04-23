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
	"reflect"
	"testing"
)

func TestClient_GetInfo(t *testing.T) {
	c, _, _ := launchCluster(t, 3)
	defer c.shutdown()

	c.waitForCommitted(2)

	for _, r := range c.rr {
		want := r.info()
		client := NewClient(c.id2Addr(r.nid))
		client.dial = r.dialFn
		got, err := client.GetInfo()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Logf(" got %#v", got)
			t.Logf("want %#v", want)
			t.Fatal("info is not same")
		}
	}
}
