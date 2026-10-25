package proxy

import (
	"sync"
	"testing"
	"time"
)

func TestBreakerCycle(t *testing.T) {
	now := time.Unix(1_000, 0)
	b := NewBreaker(2, time.Second)
	b.now = func() time.Time { return now }

	if !b.Allow() {
		t.Fatal("closed")
	}
	b.Failure()
	if !b.Allow() {
		t.Fatal("still closed after one failure")
	}
	b.Failure()
	if b.Allow() {
		t.Fatal("expected open")
	}

	now = now.Add(time.Second)
	if !b.Allow() {
		t.Fatal("expected half-open probe")
	}
	if b.Allow() {
		t.Fatal("expected a single probe")
	}
	b.Failure()
	if b.Allow() {
		t.Fatal("probe failure should re-open")
	}

	now = now.Add(time.Second)
	if !b.Allow() {
		t.Fatal("expected a second probe")
	}
	b.Success()
	if !b.Allow() {
		t.Fatal("expected closed after success")
	}
}

func TestBreakerParallel(t *testing.T) {
	b := NewBreaker(50, time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				if b.Allow() {
					b.Success()
				}
			}
		}()
	}
	wg.Wait()
}

func TestRegistryReuses(t *testing.T) {
	r := NewRegistry(3, time.Second)
	a := r.Get("http://upstream")
	b := r.Get("http://upstream")
	if a != b {
		t.Fatal("expected the same breaker")
	}
}
