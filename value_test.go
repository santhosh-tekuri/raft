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
	"fmt"
	"io/ioutil"
	"testing"
)

// todo: test openValue errors

func TestValue(t *testing.T) {
	dir, err := ioutil.TempDir(tempDir, "val")
	if err != nil {
		t.Fatal(err)
	}
	v, err := openValue(dir, ".val")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureValue(v, 0, 0); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err = v.set(i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
		if err = ensureValue(v, i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
		if v, err = openValue(dir, ".val"); err != nil {
			t.Fatal(err)
		}
		if err = ensureValue(v, i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkValue_set(b *testing.B) {
	dir, err := ioutil.TempDir(tempDir, "val")
	if err != nil {
		b.Fatal(err)
	}
	v, err := openValue(dir, ".val")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if err := v.set(uint64(n), uint64(n)); err != nil {
			b.Fatal(err)
		}
	}
}

func ensureValue(v *value, w1, w2 uint64) error {
	g1, g2 := v.get()
	if g1 != w1 {
		return fmt.Errorf("v1: got %v, want %v", g1, w1)
	}
	if g2 != w2 {
		return fmt.Errorf("v2: got %v, want %v", g2, w2)
	}
	return nil
}
