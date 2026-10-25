package proxy

import (
	"sync"
	"time"
)

type state int

const (
	stateClosed state = iota
	stateOpen
	stateHalfOpen
)

// Breaker is a per-upstream circuit breaker.
// Closed failures trip it. While open, calls are rejected until cooldown.
// The next call is a single half-open probe.
type Breaker struct {
	mu        sync.Mutex
	state     state
	failures  int
	threshold int
	cooldown  time.Duration
	opened    time.Time
	probe     bool
	now       func() time.Time
}

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 5
	}
	if cooldown <= 0 {
		cooldown = 5 * time.Second
	}
	return &Breaker{
		threshold: threshold,
		cooldown:  cooldown,
		now:       time.Now,
	}
}

func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	switch b.state {
	case stateClosed:
		return true
	case stateOpen:
		if now.Sub(b.opened) < b.cooldown {
			return false
		}
		b.state = stateHalfOpen
		b.probe = true
		return true
	default:
		if b.probe {
			return false
		}
		b.probe = true
		return true
	}
}

func (b *Breaker) Success() {
	b.mu.Lock()
	b.failures = 0
	b.probe = false
	b.state = stateClosed
	b.mu.Unlock()
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probe = false
	if b.state == stateHalfOpen {
		b.trip(b.now())
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.trip(b.now())
	}
}

func (b *Breaker) Ignore() {
	b.mu.Lock()
	b.probe = false
	b.mu.Unlock()
}

func (b *Breaker) trip(now time.Time) {
	b.state = stateOpen
	b.opened = now
}

// Registry reuses breakers across route refreshes so a snapshot swap does
// not close an open circuit.
type Registry struct {
	mu        sync.Mutex
	m         map[string]*Breaker
	threshold int
	cooldown  time.Duration
}

func NewRegistry(threshold int, cooldown time.Duration) *Registry {
	return &Registry{
		m:         make(map[string]*Breaker),
		threshold: threshold,
		cooldown:  cooldown,
	}
}

func (r *Registry) Get(upstream string) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.m[upstream]; ok {
		return b
	}
	b := NewBreaker(r.threshold, r.cooldown)
	r.m[upstream] = b
	return b
}
