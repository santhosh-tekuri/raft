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
		if (i+1)%10 == 0 {
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
		if (i+1)%10 == 0 {
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
			for i := 0; i <= 1; i++ {
				last := l.LastIndex()
				if last != test.lastIndex {
					t.Fatalf("last: got %d, want %d", last, test.lastIndex)
				}
				count := l.Count()
				if count != test.count {
					t.Fatalf("count: got %d, want %d", count, test.count)
				}
				if test.count > 0 {
					checkGet(t, l, 0, test.lastIndex)
				}
				if got := numSegments(l); got != test.numSegments {
					t.Fatalf("numSegments: got %d, want %d", got, test.numSegments)
				}
				l = restart(t, l)
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
			for i := 0; i <= 1; i++ {
				last := l.LastIndex()
				if last != 29 {
					t.Fatalf("last: got %d, want %d", last, 29)
				}
				count := l.Count()
				if count != test.count {
					t.Fatalf("count: got %d, want %d", count, test.count)
				}
				if test.count > 0 {
					checkGet(t, l, 29-test.count+1, 29)
				}
				if got := numSegments(l); got != test.numSegments {
					t.Fatalf("numSegments: got %d, want %d", got, test.numSegments)
				}
				l = restart(t, l)
			}
		})
	}
}

func TestLog_RemoveLTE_RemoveGTE(t *testing.T) {
	tests := []struct {
		name        string
		lte         uint64
		gte         uint64
		numSegments int
		lastIndex   uint64
		count       uint64
	}{
		{"lte==gte", 9, 9, 1, 8, 0},
		{"lte>gte", 9, 8, 1, 7, 0},
		{"lte>>gte", 9, 5, 1, 4, 0},
		{"lte==gte", 5, 7, 1, 6, 7},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l := newLog(t, 10, 1024*1024)
			defer os.RemoveAll(l.dir)
			appendEntries(t, l, 0, 29)
			if err := l.RemoveLTE(test.lte); err != nil {
				t.Fatal(err)
			}
			if err := l.RemoveGTE(test.gte); err != nil {
				t.Fatal(err)
			}
			for i := 0; i <= 1; i++ {
				last := l.LastIndex()
				if last != test.lastIndex {
					t.Fatalf("last: got %d, want %d", last, test.lastIndex)
				}
				count := l.Count()
				if count != test.count {
					t.Fatalf("count: got %d, want %d", count, test.count)
				}
				if test.count > 0 {
					checkGet(t, l, last-test.count+1, last)
				}
				if got := numSegments(l); got != test.numSegments {
					t.Fatalf("numSegments: got %d, want %d", got, test.numSegments)
				}
				l = restart(t, l)
			}
		})
	}
}

func TestLog_Get_notFoundError(t *testing.T) {
	l := newLog(t, 10, 1024*1024)
	defer os.RemoveAll(l.dir)
	appendEntries(t, l, 0, 35)
	checkGet(t, l, 0, 35)

	type test struct {
		i        uint64
		notFound bool
		b        []byte
	}
	tests := []test{
		{100, true, nil},
		{40, true, nil},
		{39, false, nil},
		{36, false, nil},
	}
	runTest := func(test test) {
		t.Run(fmt.Sprintf("get(%d)", test.i), func(t *testing.T) {
			for i := 0; i <= 1; i++ {
				b, err := l.Get(test.i)
				if l.IsNotFound(err) != test.notFound {
					t.Fatalf("isNotFound: got %v, want %v", l.IsNotFound(err), test.notFound)
				}
				if !test.notFound {
					if !bytes.Equal(b, test.b) {
						t.Fatalf("entry: got %v, want %v", b, test.b)
					}
				}
				l = restart(t, l)
			}
		})
	}
	for _, test := range tests {
		runTest(test)
	}

	if err := l.RemoveGTE(33); err != nil {
		t.Fatal(err)
	}
	checkGet(t, l, 0, 32)
	tests = []test{
		{100, true, nil},
		{40, true, nil},
		{39, false, nil},
		{33, false, message(33)},
	}
	for _, test := range tests {
		runTest(test)
	}

	if err := l.RemoveLTE(15); err != nil {
		t.Fatal(err)
	}
	checkGet(t, l, 10, 32)
	tests = []test{
		{100, true, nil},
		{40, true, nil},
		{39, false, nil},
		{33, false, message(33)},
		{0, true, nil},
		{9, true, nil},
		{10, false, message(10)},
		{13, false, message(13)},
	}
	for _, test := range tests {
		runTest(test)
	}
}

func TestLog_WriteTo(t *testing.T) {
	l := newLog(t, 10, 1024*1024)
	defer os.RemoveAll(l.dir)
	appendEntries(t, l, 0, 35)
	checkGet(t, l, 0, 35)

	last := l.LastIndex()
	if last != 35 {
		t.Fatalf("last=%d, want %d", last, 35)
	}
	count := l.Count()
	if count != 36 {
		t.Fatalf("count=%d, want %d", count, 36)
	}

	type test struct {
		name string
		i    uint64
		n    uint64
		b    []byte
	}
	tests := []test{
		{"a", 0, count, messages(0, last)},
		{"b", 5, count - 5, messages(5, last)},
		{"c", 9, count - 9, messages(9, last)},
		{"d", 10, count - 10, messages(10, last)},
		{"e", 15, count - 15, messages(15, last)},
	}
	runTest := func(test test) {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i <= 1; i++ {
				buf := new(bytes.Buffer)
				if err := l.WriteTo(buf, test.i, test.n); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(buf.Bytes(), test.b) {
					t.Log("got:", string(buf.Bytes()))
					t.Log("want:", string(test.b))
					t.Fatal()
				}
				l = restart(t, l)
			}
		})
	}
	for _, test := range tests {
		runTest(test)
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

func messages(first, last uint64) []byte {
	buf := new(bytes.Buffer)
	for first <= last {
		buf.Write(message(first))
		first++
	}
	return buf.Bytes()
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
