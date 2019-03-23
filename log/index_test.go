package log

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile("", ".index")
	if err != nil {
		t.Fatalf("tempFile: %v", err)
	}
	if err := os.Remove(f.Name()); err != nil {
		t.Fatalf("removeTempFile: %v", err)
	}
	cap := uint64(6)
	idx, err := newIndex(f.Name(), cap)
	if err != nil {
		t.Fatal(err)
	}
	n, dataSize, sizes := uint64(0), int64(0), []int{5, 12, 31, 75, 101, 120}
	check := func(t *testing.T) {
		t.Helper()
		if idx.n != n {
			t.Fatalf("idx.n: got %d, want %d", idx.n, n)
		}
		if idx.dataSize != dataSize {
			t.Fatalf("idx.dataSize: got %d, want %d", idx.dataSize, dataSize)
		}
		off := int64(0)
		for i := uint64(0); i < n; i++ {
			eoff, elen, err := idx.entry(i)
			if err != nil {
				t.Fatalf("idx.entry(%d): %v", i, err)
			}
			if eoff != off {
				t.Fatalf("offset(%d): got %d, want %d", i, eoff, off)
			}
			if elen != sizes[i] {
				t.Fatalf("size(%d): got %d, want %d", i, elen, sizes[i])
			}
			off += int64(sizes[i])
		}
		full := n == cap
		if got := idx.isFull(); got != full {
			t.Fatalf("idx.isFull: got %v, want %v", got, full)
		}
	}
	reopen := func(t *testing.T) {
		t.Helper()
		if err := idx.close(); err != nil {
			t.Fatal(err)
		}
		idx, err = newIndex(f.Name(), cap)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, size := range sizes {
		t.Run(strconv.Itoa(int(n)), func(t *testing.T) {
			check(t)
			if err := idx.append(size); err != nil {
				t.Fatalf("append: %v", err)
			}
			n++
			dataSize += int64(size)
			check(t)
			if n%2 == 0 {
				reopen(t)
				check(t)
			}
		})
	}
	if err := idx.truncate(3); err != nil {
		t.Fatal(err)
	}
	n, dataSize = 3, int64(sizes[0]+sizes[1]+sizes[2])
	check(t)
	reopen(t)
	check(t)

	_ = idx.close()
	_ = idx.remove()
}
