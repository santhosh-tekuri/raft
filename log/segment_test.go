package log

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, err := ioutil.TempDir("", "segment")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	off, cap := uint64(5), 6
	opt := Options{cap, 1024 * 1024}
	s, err := newSegment(dir, off, opt)
	if err != nil {
		t.Fatal(err)
	}
	n, dataSize, data := uint64(0), int64(0), []string{"a", "bb", "ccc", "dddd", "eeeee", "ffffff"}
	check := func(t *testing.T) {
		t.Helper()
		if s.idx.n != n {
			t.Fatalf("s.idx.n: got %d, want %d", s.idx.n, n)
		}
		if s.idx.dataSize != dataSize {
			t.Fatalf("idx.dataSize: got %d, want %d", s.idx.dataSize, dataSize)
		}
		for i := uint64(0); i < n; i++ {
			j := off + uint64(i)
			b := s.get(j, 1)
			if string(b) != data[i] {
				t.Fatalf("s.get(%d): got %s, want %s", j, string(b), data[i])
			}
		}
		full := n == uint64(cap)
		if got := s.idx.isFull(); got != full {
			t.Fatalf("s.idx.isFull: got %v, want %v", got, full)
		}
	}
	reopen := func(t *testing.T) {
		t.Helper()
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
		s, err = newSegment(dir, off, opt)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i, d := range data {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			check(t)
			if err := s.append([]byte(d)); err != nil {
				t.Fatalf("append(%s): %v", d, err)
			}
			n++
			dataSize += int64(len(d))
			check(t)
			if n%2 == 0 {
				reopen(t)
				check(t)
			}
		})
	}
	if err := s.removeGTE(off + uint64(3)); err != nil {
		t.Fatal(err)
	}
	n, dataSize = 3, int64(len(data[0])+len(data[1])+len(data[2]))
	check(t)
	reopen(t)
	check(t)

	_ = s.close()
	_ = s.remove()
}
