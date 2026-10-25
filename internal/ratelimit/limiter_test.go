package ratelimit

import (
	"sync"
	"testing"
	"time"
)

func TestBurstThenRefill(t *testing.T) {
	var now int64 = 1_000_000_000
	l := New()
	l.now = func() int64 { return now }

	// 60 rpm -> 1 token/s, burst of 2.
	if !l.Allow(7, 60) || !l.Allow(7, 60) {
		t.Fatal("expected the burst to be allowed")
	}
	if l.Allow(7, 60) {
		t.Fatal("expected the third call to be denied")
	}
	now += int64(time.Second)
	if !l.Allow(7, 60) {
		t.Fatal("expected one token after a second")
	}
	if l.Allow(7, 60) {
		t.Fatal("expected the refilled token to be spent")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l := New()
	if !l.Allow(1, 60) || !l.Allow(2, 60) {
		t.Fatal("distinct ids share a bucket")
	}
}

func TestUnlimited(t *testing.T) {
	l := New()
	for i := 0; i < 10; i++ {
		if !l.Allow(1, 0) {
			t.Fatal("rpm 0 should be unlimited")
		}
	}
}

func TestSweep(t *testing.T) {
	var now int64 = 10_000
	l := New()
	l.now = func() int64 { return now }
	if !l.Allow(4, 60) {
		t.Fatal("allow")
	}
	if n := l.Sweep(now + 1); n != 1 {
		t.Fatalf("swept %d", n)
	}
	// A new bucket is full again, which shows the old one was dropped.
	if !l.Allow(4, 60) || !l.Allow(4, 60) {
		t.Fatal("expected a fresh burst after sweep")
	}
}

func TestAllowParallel(t *testing.T) {
	l := New()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			for n := 0; n < 200; n++ {
				l.Allow(id, 1_000_000)
			}
		}(uint64(i))
	}
	wg.Wait()
}

func BenchmarkAllow(b *testing.B) {
	l := New()
	l.Allow(1, 6_000_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow(1, 6_000_000)
	}
}

func BenchmarkAllowParallel(b *testing.B) {
	l := New()
	l.Allow(1, 6_000_000)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow(1, 6_000_000)
		}
	})
}
