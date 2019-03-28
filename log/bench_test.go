package log

import (
	"testing"
)

func BenchmarkLog_Get(b *testing.B) {
	l := newLog(b, 16*1024*1024, true)
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
	l := newLog(b, 16*1024*1024, true)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if err := l.Append([]byte("data")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLog_AppendSync(b *testing.B) {
	bench := func(b *testing.B, together bool) {
		l := newLog(b, 16*1024*1024, together)
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
	b.Run("syncTogether", func(b *testing.B) {
		bench(b, true)
	})
	b.Run("syncSeparate", func(b *testing.B) {
		bench(b, false)
	})
}
