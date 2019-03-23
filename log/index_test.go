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
	cap := 6
	idx, err := newIndex(f.Name(), cap)
	if err != nil {
		t.Fatal(err)
	}
	n, dataSize, sizes := 0, int64(0), []int64{5, 12, 31, 75, 101, 120}
	check := func(t *testing.T) {
		t.Helper()
		if idx.n != n {
			t.Fatalf("idx.n: got %d, want %d", idx.n, n)
		}
		if idx.dataSize() != dataSize {
			t.Fatalf("idx.dataSize: got %d, want %d", idx.dataSize(), dataSize)
		}
		off := int64(0)
		for i := 0; i < n; i++ {
			if got := idx.offset(i); got != off {
				t.Fatalf("idx.offset(%d): got %d, want %d", i, got, off)
			}
			off += sizes[i]
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
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			check(t)
			if err := idx.append(dataSize + size); err != nil {
				t.Fatalf("append: %v", err)
			}
			n++
			dataSize += size
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
	n, dataSize = 3, sizes[0]+sizes[1]+sizes[2]
	check(t)
	reopen(t)
	check(t)

	_ = idx.close()
	_ = idx.remove()
}
