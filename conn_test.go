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

import "testing"

// tests that dialed conn is validated for cid and nid
func TestConnPool_getConn_IdentityError(t *testing.T) {
	// launch single node cluster
	c1, ldr, _ := launchCluster(t, 1)
	defer c1.shutdown()

	// start a raft instance without bootstrap with different cluster id
	c2 := newCluster(t)
	r2 := c2.launch(2, false)[2]
	defer c2.shutdown()

	// add node in cluster 2 as nonvoter in cluster1
	if err := c1.waitAddNonvoter(ldr, 2, c2.id2Addr(2), false); err != nil {
		t.Fatal(err)
	}

	// ensure that ldr detects that nonvoter is does not belong to cluster
	// and treats it as unreachable
	err := c1.waitUnreachableDetected(ldr, r2)
	if _, ok := err.(IdentityError); !ok {
		c1.Fatalf("waitUnreachableDetected: got %v, want IdentityError", err)
	}
}

// tests that addr update in config is picked up by connPool
func TestConnPool_getConn_ConfigAddrUpdate(t *testing.T) {
	// launch 3 node cluster
	c, ldr, flrs := launchCluster(t, 3)
	defer c.shutdown()

	// stop one of follower
	c.shutdown(flrs[0])

	// wait for leader to detect that follower is unreachable
	_ = c.waitUnreachableDetected(ldr, flrs[0])

	// restart follower at different address
	c.ports[flrs[0].nid] = 9999
	c.restart(flrs[0])

	// wait until leader becomes commit ready
	c.waitCommitReady(ldr)

	// submit ChangeConfig with new addr
	config := c.info(ldr).Configs.Latest
	if err := config.SetAddr(flrs[0].nid, c.id2Addr(flrs[0].nid)); err != nil {
		t.Fatal(err)
	}
	c.ensure(waitTask(ldr, ChangeConfig(config), c.longTimeout))

	// wait for leader to detect that follower is reachable
	c.waitReachableDetected(ldr, flrs[0])
}

// tests that addr update from Resolver is picked up by connPool
func TestConnPool_getConn_Resolver(t *testing.T) {
	// launch 3 node cluster with Resolver set
	c := newCluster(t)
	c.opt.Resolver = c
	ldr, flrs := c.ensureLaunch(3)
	defer c.shutdown()

	// stop one of follower
	c.shutdown(flrs[0])

	// wait for leader to detect that follower is unreachable
	_ = c.waitUnreachableDetected(ldr, flrs[0])

	// restart follower at different address with resolver addr updated
	c.resolverMu.Lock()
	c.ports[flrs[0].nid] = 9999
	c.resolverMu.Unlock()
	c.restart(flrs[0])

	// wait for leader to detect that follower is reachable at new addr
	c.waitReachableDetected(ldr, flrs[0])
}
