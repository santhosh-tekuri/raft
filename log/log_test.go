package log

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"reflect"
	"testing"
)

func TestOpen(t *testing.T) {
	l := newLog(t, 1024)

	assertInt(t, "available", l.last.available(), 1024-3*8)
	assertInt(t, "numSegments", numSegments(l), 1)
	assertUint64(t, "prevIndex", l.PrevIndex(), 0)
	assertUint64(t, "lastIndex", l.LastIndex(), 0)
	assertUint64(t, "count", l.Count(), 0)

	_, err := l.Get(0)
	checkErrNotFound(t, err)
	checkPanic(t, func() {
		_, _ = l.Get(1) // beyond last segment
	})
}

func TestSegmentSize(t *testing.T) {
	l := newLog(t, 1024)

	b := make([]byte, l.last.available()+1)
	rand.Read(b)
	if err := l.Append(b); err != ErrExceedsSegmentSize {
		t.Fatalf("got %v, want ErrExceedsSegmentSize", err)
	}
	if err := l.Append(b[:len(b)-1]); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(b); err != nil {
		t.Fatal(err)
	}
	assertInt(t, "numSegments", numSegments(l), 2)
	assertInt(t, "segmentSize", l.opt.SegmentSize, 1025)
}

func TestLog_Get(t *testing.T) {
	l := newLog(t, 1024)

	n, numSeg := uint64(0), 1
	for i := 0; i <= 1; i++ {
		for numSegments(l) != numSeg+2 {
			n++
			appendEntry(t, l)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
		numSeg += 2

		for i := 0; i <= 1; i++ {
			assertInt(t, "numSegments", numSegments(l), numSeg)
			assertUint64(t, "prevIndex", l.PrevIndex(), 0)
			assertUint64(t, "lastIndex", l.LastIndex(), n)
			assertUint64(t, "count", l.Count(), n)
			checkGet(t, l)

			_, err := l.Get(0)
			checkErrNotFound(t, err)
			checkPanic(t, func() {
				_, _ = l.Get(n + 1) // beyond last segment
			})
			l = reopen(t, l)
		}
	}
}

func TestLog_GetN(t *testing.T) {
	l := newLog(t, 1024)

	var n uint64
	for numSegments(l) != 4 {
		n++
		appendEntry(t, l)
	}
	for i := 0; i < 10; i++ {
		n++
		appendEntry(t, l)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}

	checkGetN(t, l, 1, 0, nil)
	checkGetN(t, l, 5, 0, nil)

	checkGetN(t, l, 1, 1, msg(1))
	checkGetN(t, l, 5, 1, msg(5))
	checkGetN(t, l, l.LastIndex(), 1, msg(l.LastIndex()))

	checkGetN(t, l, 1, n, msgs(1, n))
	checkGetN(t, l, 5, n-4, msgs(5, n-4))

	_, err := l.GetN(0, 3)
	checkErrNotFound(t, err)

	segs := getSegments(l)
	lastSeg := segs[len(segs)-1]

	checkGetN(t, l, segs[1], 1, msg(segs[1]))
	checkGetN(t, l, segs[1]+1, 1, msg(segs[1]+1))
	checkGetN(t, l, segs[2], 1, msg(segs[2]))
	checkGetN(t, l, segs[2]+1, 1, msg(segs[2]+1))
	checkGetN(t, l, lastSeg, 1, msg(lastSeg))
	checkGetN(t, l, lastSeg+1, 1, msg(lastSeg+1))

	checkGetN(t, l, segs[1], 5, msgs(segs[1], 5))
	checkGetN(t, l, segs[1]+1, 5, msgs(segs[1]+1, 5))
	checkGetN(t, l, segs[2], 5, msgs(segs[2], 5))
	checkGetN(t, l, segs[2]+1, 5, msgs(segs[2]+1, 5))
	checkGetN(t, l, lastSeg, 5, msgs(lastSeg, 5))
	checkGetN(t, l, lastSeg+1, 5, msgs(lastSeg+1, 5))

	checkGetN(t, l, segs[1]-5, 10, msgs(segs[1]-5, 10))
	checkGetN(t, l, segs[2]-5, 10, msgs(segs[2]-5, 10))
	checkGetN(t, l, lastSeg-5, 10, msgs(lastSeg-5, 10))

	from, to := segs[1], lastSeg
	checkGetN(t, l, from, to-from, msgs(from, to-from))
	from, to = segs[1], lastSeg+5
	checkGetN(t, l, from, to-from, msgs(from, to-from))
	from, to = segs[1]-5, lastSeg
	checkGetN(t, l, from, to-from, msgs(from, to-from))
	from, to = segs[1]-5, lastSeg+5
	checkGetN(t, l, from, to-from, msgs(from, to-from))

	checkPanic(t, func() {
		_, _ = l.GetN(l.LastIndex(), 2)
	})
	checkPanic(t, func() {
		_, _ = l.GetN(1, 100000)
	})
	checkPanic(t, func() {
		_, _ = l.GetN(20, 100000)
	})
}

func TestLog_ViewAt(t *testing.T) {
	l := newLog(t, 1024)

	var n uint64
	for numSegments(l) != 4 {
		n++
		appendEntry(t, l)
	}
	for i := 0; i < 10; i++ {
		n++
		appendEntry(t, l)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}

	checkView := func(v *Log, segsWant []uint64) {
		t.Helper()
		segs := getSegments(v)
		if !reflect.DeepEqual(segs, segsWant) {
			t.Log("got:", segs)
			t.Log("want:", segsWant)
			t.Fatal()
		}
		checkGet(t, v)
	}

	segs := getSegments(l)
	nseg := len(segs)
	fmt.Println(getSegments(l))
	checkView(l.View(), segs)
	checkView(l.ViewAt(0, l.LastIndex()-10), segs)
	checkView(l.ViewAt(0, segs[nseg-1]), segs[:nseg-1])
	checkView(l.ViewAt(0, segs[nseg-1]-10), segs[:nseg-1])
	checkView(l.ViewAt(5, segs[nseg-1]-10), segs[:nseg-1])
	checkView(l.ViewAt(segs[1], segs[nseg-1]-10), segs[1:nseg-1])

	v := l.View()
	started, stop := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		for {
			select {
			case <-stop:
				close(stop)
				return
			default:
				checkGet(t, v)
			}
		}
	}()
	<-started
	for i := l.LastIndex() + 1; i <= 1000; i++ {
		if err := l.Append(msg(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	stop <- struct{}{}
	<-stop
	checkGet(t, l)
}

func TestLog_RemoveLTE(t *testing.T) {
	newLog := func() *Log {
		t.Helper()
		l := newLog(t, 1024)

		var n uint64
		for numSegments(l) != 4 {
			n++
			appendEntry(t, l)
		}
		for i := 0; i < 10; i++ {
			n++
			appendEntry(t, l)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
		return l
	}
	removeLTE := func(i uint64, want []uint64) {
		t.Helper()
		l := newLog()
		segs := getSegments(l)
		lastIndex := l.LastIndex()
		err := l.RemoveLTE(i)
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j <= 1; j++ {
			if l.LastIndex() != lastIndex {
				t.Fatalf("lastIndex=%d, want %d", l.LastIndex(), lastIndex)
			}
			got := getSegments(l)
			if !reflect.DeepEqual(got, want) {
				t.Logf("%v %d removeLTE(%d) -> %v", segs, lastIndex, i, got)
				t.Log("want:", want)
				t.Fatal()
			}
			t.Logf("%v %d removeLTE(%d) -> %v", segs, lastIndex, i, got)
			l = reopen(t, l)
		}
	}

	l := newLog()
	segs := getSegments(l)
	nseg := len(segs)
	lastIndex := l.LastIndex()
	if err := l.RemoveLTE(segs[nseg-2]); err != nil {
		t.Fatal(err)
	}
	// remaining segs[nseg-2:]
	checkGet(t, l)
	_, err := l.Get(0)
	checkErrNotFound(t, err)
	_, err = l.Get(25)
	checkErrNotFound(t, err)
	_, err = l.Get(l.PrevIndex())
	checkErrNotFound(t, err)
	checkPanic(t, func() {
		_, _ = l.Get(l.LastIndex() + 1000) // beyond last segment
	})

	checkGetN(t, l, l.PrevIndex()+1, l.Count(), msgs(l.PrevIndex()+1, l.Count()))
	_, err = l.GetN(0, 10)
	checkErrNotFound(t, err)
	_, err = l.GetN(l.PrevIndex()-10, 50)
	checkErrNotFound(t, err)
	_, err = l.GetN(l.PrevIndex(), 50)
	checkErrNotFound(t, err)

	removeLTE(lastIndex+999, segs[nseg-1:])
	removeLTE(lastIndex+100, segs[nseg-1:])
	removeLTE(lastIndex+1, segs[nseg-1:])
	removeLTE(lastIndex, segs[nseg-1:])

	removeLTE(lastIndex-1, segs[nseg-1:])
	removeLTE(lastIndex-10, segs[nseg-1:])
	removeLTE(segs[nseg-1]+10, segs[nseg-1:])
	removeLTE(segs[nseg-1]+1, segs[nseg-1:])
	removeLTE(segs[nseg-1], segs[nseg-1:])

	removeLTE(segs[nseg-1]-1, segs[nseg-2:])
	removeLTE(segs[nseg-1]-10, segs[nseg-2:])
	removeLTE(segs[nseg-2]+10, segs[nseg-2:])
	removeLTE(segs[nseg-2]+1, segs[nseg-2:])
	removeLTE(segs[nseg-2], segs[nseg-2:])

	removeLTE(segs[1]+1, segs[1:])
	removeLTE(segs[1], segs[1:])

	removeLTE(segs[1]-1, segs)
	removeLTE(segs[1]-10, segs)
	removeLTE(10, segs)
	removeLTE(1, segs)
	removeLTE(0, segs)
}

func TestLog_RemoveGTE(t *testing.T) {
	newLog := func() *Log {
		t.Helper()
		l := newLog(t, 1024)

		var n uint64
		for numSegments(l) != 4 {
			n++
			appendEntry(t, l)
		}
		for i := 0; i < 10; i++ {
			n++
			appendEntry(t, l)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
		return l
	}
	removeGTE := func(i uint64, want []uint64, lastIndex uint64) {
		t.Helper()
		l := newLog()
		segs := getSegments(l)
		originalLastIndex := l.LastIndex()
		err := l.RemoveGTE(i)
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j <= 1; j++ {
			if l.LastIndex() != lastIndex {
				t.Fatalf("lastIndex=%d, want %d", l.LastIndex(), lastIndex)
			}
			got := getSegments(l)
			if !reflect.DeepEqual(got, want) {
				t.Logf("%v %d removeGTE(%d) -> %v %d", segs, originalLastIndex, i, got, l.LastIndex())
				t.Log("want:", want)
				t.Fatal()
			}
			t.Logf("%v %d removeGTE(%d) -> %v %d", segs, originalLastIndex, i, got, l.LastIndex())
			l = reopen(t, l)
		}
	}

	l := newLog()
	segs := getSegments(l)
	nseg := len(segs)
	lastIndex := l.LastIndex()

	removeGTE(lastIndex+999, segs, lastIndex)
	removeGTE(lastIndex+100, segs, lastIndex)
	removeGTE(lastIndex+1, segs, lastIndex)
	removeGTE(lastIndex, segs, lastIndex-1)
	removeGTE(lastIndex-1, segs, lastIndex-2)
	removeGTE(lastIndex-5, segs, lastIndex-6)

	removeGTE(segs[nseg-1]+5, segs, segs[nseg-1]+4)
	removeGTE(segs[nseg-1]+1, segs[:nseg-1], segs[nseg-1])
	removeGTE(segs[nseg-1], segs[:nseg-1], segs[nseg-1]-1)
	removeGTE(segs[nseg-1]-5, segs[:nseg-1], segs[nseg-1]-6)

	removeGTE(segs[nseg-2]+5, segs[:nseg-1], segs[nseg-2]+4)
	removeGTE(segs[nseg-2]+1, segs[:nseg-2], segs[nseg-2])
	removeGTE(segs[nseg-2], segs[:nseg-2], segs[nseg-2]-1)
	removeGTE(segs[nseg-2]-5, segs[:nseg-2], segs[nseg-2]-6)

	removeGTE(5, []uint64{0}, 4)
	removeGTE(1, []uint64{0}, 0)
	removeGTE(0, []uint64{0}, 0)
}

func TestLog_RemoveLTE_RemoveGTE(t *testing.T) {
	newLog := func() *Log {
		t.Helper()
		l := newLog(t, 1024)

		var n uint64
		for numSegments(l) != 4 {
			n++
			appendEntry(t, l)
		}
		for i := 0; i < 10; i++ {
			n++
			appendEntry(t, l)
		}
		if err := l.RemoveLTE(100); err != nil {
			t.Fatal(err)
		}
		return l
	}
	removeGTE := func(i uint64, want []uint64, lastIndex uint64) {
		t.Helper()
		l := newLog()
		segs := getSegments(l)
		originalLastIndex := l.LastIndex()
		err := l.RemoveGTE(i)
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j <= 1; j++ {
			if l.LastIndex() != lastIndex {
				t.Fatalf("lastIndex=%d, want %d", l.LastIndex(), lastIndex)
			}
			got := getSegments(l)
			if !reflect.DeepEqual(got, want) {
				t.Logf("%v %d removeGTE(%d) -> %v %d", segs, originalLastIndex, i, got, l.LastIndex())
				t.Log("want:", want)
				t.Fatal()
			}
			t.Logf("%v %d removeGTE(%d) -> %v %d", segs, originalLastIndex, i, got, l.LastIndex())
			l = reopen(t, l)
		}
	}

	l := newLog()
	segs := getSegments(l)
	nseg := len(segs)
	lastIndex := l.LastIndex()

	removeGTE(lastIndex+999, segs, lastIndex)
	removeGTE(lastIndex+100, segs, lastIndex)
	removeGTE(lastIndex+1, segs, lastIndex)
	removeGTE(lastIndex, segs, lastIndex-1)
	removeGTE(lastIndex-1, segs, lastIndex-2)
	removeGTE(lastIndex-5, segs, lastIndex-6)

	removeGTE(segs[nseg-1]+5, segs, segs[nseg-1]+4)
	removeGTE(segs[nseg-1]+1, segs[:nseg-1], segs[nseg-1])
	removeGTE(segs[nseg-1], segs[:nseg-1], segs[nseg-1]-1)
	removeGTE(segs[nseg-1]-5, segs[:nseg-1], segs[nseg-1]-6)

	removeGTE(segs[nseg-2]+5, segs[:nseg-1], segs[nseg-2]+4)
	removeGTE(segs[nseg-2]+1, segs[:nseg-2], segs[nseg-2])
	removeGTE(segs[nseg-2], segs[:nseg-2], segs[nseg-2]-1)
	removeGTE(segs[nseg-2]-5, segs[:nseg-2], segs[nseg-2]-6)

	removeGTE(segs[0]+5, segs[:1], segs[0]+4)
	removeGTE(segs[0]+1, segs[:1], segs[0])
	removeGTE(segs[0], []uint64{segs[0] - 1}, segs[0]-1)
	removeGTE(segs[0]-5, []uint64{segs[0] - 6}, segs[0]-6)

	removeGTE(5, []uint64{4}, 4)
	removeGTE(1, []uint64{0}, 0)
	removeGTE(0, []uint64{0}, 0)
}

var tempDir string

func TestMain(M *testing.M) {
	temp, err := ioutil.TempDir("", "log")
	if err != nil {
		os.Exit(1)
	}
	tempDir = temp
	code := M.Run()
	_ = os.RemoveAll(tempDir)
	os.Exit(code)
}

// helpers -----------------------------------------

func newLog(tb testing.TB, size int) *Log {
	tb.Helper()
	dir, err := ioutil.TempDir(tempDir, "log")
	if err != nil {
		tb.Fatal(err)
	}
	l, err := Open(dir, 0700, Options{0600, size})
	if err != nil {
		tb.Fatal(err)
	}
	return l
}

func reopen(t *testing.T, l *Log) *Log {
	t.Helper()
	t.Log("reopening...")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := Open(l.dir, 0700, l.opt)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func msg(i uint64) []byte {
	if i%2 == 0 {
		return []byte(fmt.Sprintf("even[%d]", i))
	}
	return []byte(fmt.Sprintf("odd[%d]", i))
}

func msgs(i uint64, n uint64) []byte {
	buf := new(bytes.Buffer)
	for n > 0 {
		buf.Write(msg(i))
		i++
		n--
	}
	return buf.Bytes()
}

func numSegments(l *Log) int {
	n := 0
	for s := l.first; s != nil; s = s.next {
		n++
	}
	return n
}

func appendEntry(t *testing.T, l *Log) {
	t.Helper()
	i := l.LastIndex() + 1
	if err := l.Append(msg(i)); err != nil {
		t.Fatalf("append(%d): %v", i, err)
	}
}

func checkGet(t *testing.T, l *Log) {
	t.Helper()

	_, err := l.Get(l.PrevIndex())
	checkErrNotFound(t, err)

	checkPanic(t, func() {
		_, _ = l.Get(l.LastIndex() + 1)
	})

	first, last := l.PrevIndex()+1, l.LastIndex()
	for first <= last {
		got, err := l.Get(first)
		if err != nil {
			t.Fatalf("get(%d): %v", first, err)
		}
		want := msg(first)
		if !bytes.Equal(got, want) {
			t.Fatalf("get(%d)=%q, want %q", first, string(got), string(want))
		}
		first++
	}
}

func checkGetN(t *testing.T, l *Log, i, n uint64, want []byte) {
	t.Helper()
	buffs, err := l.GetN(i, n)
	if err != nil {
		t.Fatal(err)
	}
	nbuffs := net.Buffers(buffs)
	buf := new(bytes.Buffer)
	if _, err := nbuffs.WriteTo(buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Log("got:", string(buf.Bytes()))
		t.Log("want:", string(want))
		t.Fatal()
	}
}

func assertUint64(t *testing.T, name string, got, want uint64) {
	t.Helper()
	if got != want {
		t.Fatalf("%s=%v, want %v", name, got, want)
	}
}

func assertInt(t *testing.T, name string, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("%s=%v, want %v", name, got, want)
	}
}

func checkErrNotFound(t *testing.T, err error) {
	t.Helper()
	if err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func checkPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	f()
}

func getSegments(l *Log) []uint64 {
	var segs []uint64
	s := l.first
	for {
		segs = append(segs, s.prevIndex)
		if s == l.last {
			break
		}
		s = s.next
	}
	return segs
}
