package stream

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type funcSink struct {
	fn func(batch []Event) error
}

func (f funcSink) Publish(ctx context.Context, batch []Event) error {
	return f.fn(batch)
}

func TestEmitDropsWhenFull(t *testing.T) {
	var dropped atomic.Int32
	p := New(1, funcSink{fn: func([]Event) error { return nil }}, Hooks{
		Dropped: func() { dropped.Add(1) },
	})
	p.Emit(Event{Path: "/a"})
	p.Emit(Event{Path: "/b"})
	if dropped.Load() != 1 {
		t.Fatalf("dropped %d", dropped.Load())
	}
}

func TestBatchFlush(t *testing.T) {
	got := make(chan Event, 1)
	p := New(8, funcSink{fn: func(batch []Event) error {
		for _, e := range batch {
			got <- e
		}
		return nil
	}}, Hooks{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	p.Emit(Event{Path: "/v1/echo", Status: 200})
	select {
	case e := <-got:
		if e.Path != "/v1/echo" || e.Status != 200 {
			t.Fatalf("%+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for flush")
	}
	cancel()
	select {
	case <-p.Done():
	case <-time.After(time.Second):
		t.Fatal("publisher did not stop")
	}
}

func TestPublishErrorHook(t *testing.T) {
	failed := make(chan struct{}, 1)
	p := New(4, funcSink{fn: func([]Event) error { return context.DeadlineExceeded }}, Hooks{
		Failed: func() {
			select {
			case failed <- struct{}{}:
			default:
			}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)
	p.Emit(Event{Path: "/x"})
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("expected failed hook")
	}
	cancel()
	<-p.Done()
}
