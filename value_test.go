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
	v, err := openValue(dir, ".val", 0600)
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
		if v, err = openValue(dir, ".val", 0600); err != nil {
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
	v, err := openValue(dir, ".val", 0600)
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
