package log

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"
)

func TestLog_Append_singleSegment(t *testing.T) {
	l := newLog(t, 1000, 1024*1024)
	defer os.RemoveAll(l.dir)
	for i := uint64(0); i < 30; i++ {
		if err := l.Append(message(i)); err != nil {
			t.Fatal(err)
		}
		if i+1%10 == 0 {
			checkGet(t, l, 0, i)
			l = restart(t, l)
			checkGet(t, l, 0, i)
		}
	}
	if got := numSegments(l); got != 1 {
		t.Fatalf("numSegments: got %d, want %d", got, 1)
	}
}

func TestLog_Append_multipleSegments(t *testing.T) {
	l := newLog(t, 10, 1024*1024)
	defer os.RemoveAll(l.dir)
	for i := uint64(0); i < 30; i++ {
		if err := l.Append(message(i)); err != nil {
			t.Fatal(err)
		}
		got, want := numSegments(l), int(math.Ceil(float64(i+1)/10.0))
		if got != want {
			t.Fatalf("numSegments after %d entries: got %d, want %d", i+1, got, want)
		}
		if i+1%10 == 0 {
			checkGet(t, l, 0, i)
			l = restart(t, l)
			got, want := numSegments(l), int(math.Ceil(float64(i+1)/10.0))
			if got != want {
				t.Fatalf("numSegments after %d entries: got %d, want %d", i+1, got, want)
			}
			checkGet(t, l, 0, i)
		}
	}
}

func TestLog_RemoveGTE(t *testing.T) {
	tests := []struct {
		name        string
		gte         uint64
		numSegments int
		lastIndex   uint64
		count       uint64
	}{
		{"largerThanLastIndex", 100, 3, 29, 30},
		{"greaterThanLastIndex", 30, 3, 29, 30},
		{"equalToLastIndex", 29, 3, 28, 29},
		{"insideLastSegment", 25, 3, 24, 25},
		{"lastSegmentOffset", 20, 3, 19, 20},
		{"prevSegmentLastIndex", 19, 2, 18, 19},
		{"insidePrevSegment", 15, 2, 14, 15},
		{"prevSegmentOffset", 10, 2, 9, 10},
		{"firstSegmentLastIndex", 9, 1, 8, 9},
		{"insideFirstSegment", 5, 1, 4, 5},
		{"firstSegmentOffset", 0, 1, 0, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l := newLog(t, 10, 1024*1024)
			defer os.RemoveAll(l.dir)
			appendEntries(t, l, 0, 29)
			if err := l.RemoveGTE(test.gte); err != nil {
				t.Fatal(err)
			}
			last, err := l.LastIndex()
			if err != nil {
				t.Fatal(err)
			}
			if last != test.lastIndex {
				t.Fatalf("last: got %d, want %d", last, test.lastIndex)
			}
			count, err := l.Count()
			if err != nil {
				t.Fatal(err)
			}
			if count != test.count {
				t.Fatalf("count: got %d, want %d", count, test.count)
			}
			if test.count > 0 {
				checkGet(t, l, 0, test.lastIndex)
			}
			if got := numSegments(l); got != test.numSegments {
				t.Fatalf("numSegments: got %d, want %d", got, test.numSegments)
			}
		})
	}
}

func TestLog_RemoveLTE(t *testing.T) {
	tests := []struct {
		name        string
		lte         uint64
		numSegments int
		count       uint64
	}{
		{"largerThanLastIndex", 100, 1, 0},
		{"greaterThanLastIndex", 30, 1, 0},
		{"equalToLastIndex", 29, 1, 0},
		{"insideLastSegment", 25, 1, 10},
		{"lastSegmentOffset", 20, 1, 10},
		{"prevSegmentLastIndex", 19, 1, 10},
		{"insidePrevSegment", 15, 2, 20},
		{"prevSegmentOffset", 10, 2, 20},
		{"firstSegmentLastIndex", 9, 2, 20},
		{"insideFirstSegment", 5, 3, 30},
		{"firstSegmentOffset", 0, 3, 30},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l := newLog(t, 10, 1024*1024)
			defer os.RemoveAll(l.dir)
			appendEntries(t, l, 0, 29)
			if err := l.RemoveLTE(test.lte); err != nil {
				t.Fatal(err)
			}
			last, err := l.LastIndex()
			if err != nil {
				t.Fatal(err)
			}
			if last != 29 {
				t.Fatalf("last: got %d, want %d", last, 29)
			}
			count, err := l.Count()
			if err != nil {
				t.Fatal(err)
			}
			if count != test.count {
				t.Fatalf("count: got %d, want %d", count, test.count)
			}
			if test.count > 0 {
				checkGet(t, l, 29-test.count+1, 29)
			}
			if got := numSegments(l); got != test.numSegments {
				t.Fatalf("numSegments: got %d, want %d", got, test.numSegments)
			}
		})
	}
}

// helpers ---------------------------------------------------------------------------

func newLog(t *testing.T, maxCount uint64, maxSize int64) *Log {
	t.Helper()
	dir, err := ioutil.TempDir("", "log")
	if err != nil {
		t.Fatal(err)
	}
	l, err := New(dir, Options{maxCount, maxSize})
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatal(err)
	}
	return l
}

func appendEntries(t *testing.T, l *Log, first, last uint64) {
	t.Helper()
	for first <= last {
		err := l.Append(message(first))
		if err != nil {
			t.Fatalf("Append(%d): %v", first, err)
		}
		first++
	}
}

func message(i uint64) []byte {
	if i%2 == 0 {
		return []byte(fmt.Sprintf("even[%d]", i))
	}
	return []byte(fmt.Sprintf("odd[%d]", i))
}

func checkGet(t *testing.T, l *Log, first, last uint64) {
	t.Helper()
	for first <= last {
		got, err := l.Get(first)
		if err != nil {
			t.Fatalf("get(%d): %v", first, err)
		}
		want := message(first)
		if !bytes.Equal(got, want) {
			t.Fatalf("get(%d): got %q, want %q", first, string(got), string(want))
		}
		first++
	}
}

func restart(t *testing.T, l *Log) *Log {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	l, err := New(l.dir, l.opt)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return l
}

func numSegments(l *Log) int {
	i, s := 0, l.last
	for s != nil {
		i++
		s = s.prev
	}
	return i
}
