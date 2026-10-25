// Package stream publishes request events without blocking the data plane.
// Emit drops the event when the buffer is full. A background goroutine
// flushes batches to the sink (Redis Streams in production).
package stream

import (
	"context"
	"time"
)

type Event struct {
	At        time.Time
	Tenant    string
	Method    string
	Path      string
	Status    int
	LatencyUs int64
	Upstream  string
}

type Sink interface {
	Publish(ctx context.Context, batch []Event) error
}

type Hooks struct {
	Dropped   func()
	Published func(n int)
	Failed    func()
}

type Publisher struct {
	ch    chan Event
	sink  Sink
	hooks Hooks
	done  chan struct{}
}

func New(buffer int, sink Sink, hooks Hooks) *Publisher {
	if buffer < 1 {
		buffer = 1
	}
	return &Publisher{
		ch:    make(chan Event, buffer),
		sink:  sink,
		hooks: hooks,
		done:  make(chan struct{}),
	}
}

// Emit enqueues e. It never blocks. A full buffer increments the drop hook.
func (p *Publisher) Emit(e Event) {
	if p == nil {
		return
	}
	select {
	case p.ch <- e:
	default:
		if p.hooks.Dropped != nil {
			p.hooks.Dropped()
		}
	}
}

func (p *Publisher) Done() <-chan struct{} {
	return p.done
}

// Run flushes at 256 events or every 5ms, whichever comes first.
// On cancellation it drains what is already buffered and returns.
func (p *Publisher) Run(ctx context.Context) {
	defer close(p.done)
	if p.sink == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	batch := make([]Event, 0, 256)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		fctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := p.sink.Publish(fctx, batch)
		cancel()
		if err != nil {
			if p.hooks.Failed != nil {
				p.hooks.Failed()
			}
		} else if p.hooks.Published != nil {
			p.hooks.Published(len(batch))
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case e := <-p.ch:
					batch = append(batch, e)
					if len(batch) == cap(batch) {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case e := <-p.ch:
			batch = append(batch, e)
			if len(batch) == cap(batch) {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}
