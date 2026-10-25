// Package ratelimit is an in-process sharded token bucket.
//
// Admission stays on the data-plane goroutine: no Redis round trip and no
// allocation after a key's bucket exists. Each gateway replica enforces the
// limit on its own, which is the latency tradeoff documented in the README.
//
// Refill is rpm/60 tokens per second. Burst is two seconds of that rate,
// with a floor of one token.
package ratelimit

import (
	"sync"
	"time"
)

const shardN = 64

type Limiter struct {
	shards [shardN]shard
	now    func() int64
}

type shard struct {
	mu      sync.Mutex
	buckets map[uint64]*bucket
}

type bucket struct {
	tokens float64
	last   int64
	rate   float64 // tokens per nanosecond
	burst  float64
	seen   int64
}

func New() *Limiter {
	l := &Limiter{now: func() int64 { return time.Now().UnixNano() }}
	for i := range l.shards {
		l.shards[i].buckets = make(map[uint64]*bucket)
	}
	return l
}

// Allow reports whether id may proceed at the given requests-per-minute.
// rpm <= 0 is treated as unlimited.
func (l *Limiter) Allow(id uint64, rpm int) bool {
	if rpm <= 0 {
		return true
	}
	rate := float64(rpm) / 60e9
	burst := float64(rpm) / 30.0
	if burst < 1 {
		burst = 1
	}
	now := l.now()
	sh := &l.shards[id&(shardN-1)]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	b := sh.buckets[id]
	if b == nil {
		b = &bucket{tokens: burst, last: now, rate: rate, burst: burst, seen: now}
		sh.buckets[id] = b
	} else {
		if b.rate != rate || b.burst != burst {
			b.rate = rate
			b.burst = burst
			if b.tokens > b.burst {
				b.tokens = b.burst
			}
		}
		if elapsed := now - b.last; elapsed > 0 {
			b.tokens += float64(elapsed) * b.rate
			if b.tokens > b.burst {
				b.tokens = b.burst
			}
			b.last = now
		}
		b.seen = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Sweep drops buckets that have not been touched since before.
// before is a unix-nano timestamp. It is called from the refresh loop,
// not the request path.
func (l *Limiter) Sweep(before int64) int {
	n := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for id, b := range sh.buckets {
			if b.seen < before {
				delete(sh.buckets, id)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}
