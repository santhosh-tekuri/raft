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

	c.waitCatchup()

	for _, r := range c.rr {
		want := c.info(r)
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

func TestClient_TakeSnapshot(t *testing.T) {
	c, ldr, _ := launchCluster(t, 3)
	defer c.shutdown()

	c.sendUpdates(ldr, 1, 10)
	c.waitFSMLen(10)

	client := NewClient(c.id2Addr(ldr.nid))
	client.dial = ldr.dialFn
	snapIndex, err := client.TakeSnapshot(0)
	if err != nil {
		t.Fatal(err)
	}
	if snapIndex != 12 {
		t.Fatalf("snapIndex=%d, want %d", snapIndex, 12)
	}
	if info := c.info(ldr); info.SnapshotIndex != snapIndex {
		t.Fatalf("info.snapshotIndex=%d", info.SnapshotIndex)
	}

	if _, err := client.TakeSnapshot(0); err != ErrNoUpdates {
		t.Fatalf("got %v, want %v", err, ErrNoUpdates)
	}
}

func TestClient_TransferLeadership(t *testing.T) {
	c, ldr, flrs := launchCluster(t, 3)
	defer c.shutdown()

	client := NewClient(c.id2Addr(ldr.nid))
	client.dial = ldr.dialFn
	if err := client.TransferLeadership(7, c.longTimeout); err != ErrTransferInvalidTarget {
		t.Fatalf("got %v, want %v", err, ErrTransferInvalidTarget)
	}
	if err := client.TransferLeadership(ldr.nid, c.longTimeout); err != ErrTransferSelf {
		t.Fatalf("got %v, want %v", err, ErrTransferSelf)
	}
	if err := client.TransferLeadership(flrs[0].nid, c.longTimeout); err != nil {
		t.Fatal(err)
	}
	newLdr := c.waitForLeader()
	if newLdr != flrs[0] {
		t.Fatalf("newLdr=%d, want %d", newLdr.nid, flrs[0].nid)
	}
}
