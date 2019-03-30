package log

import (
	"testing"
)

func BenchmarkLog_Get(b *testing.B) {
	l := newLog(b, 16*1024*1024)
	for i := 1; i < 10; i++ {
		if err := l.Append([]byte("data")); err != nil {
			b.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if _, err := l.Get(5); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLog_AppendNoSync(b *testing.B) {
	l := newLog(b, 16*1024*1024)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if err := l.Append([]byte("data")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLog_AppendSync(b *testing.B) {
	l := newLog(b, 16*1024*1024)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if err := l.Append([]byte("data")); err != nil {
			b.Fatal(err)
		}
		if err := l.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}
