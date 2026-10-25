package stream

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewRedis(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     32,
		MinIdleConns: 4,
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
}

// Wait pings Redis until it answers or ctx ends.
func Wait(ctx context.Context, c *redis.Client) error {
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		last = c.Ping(pctx).Err()
		cancel()
		if last == nil {
			return nil
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			if last == nil {
				last = ctx.Err()
			}
			return last
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// RedisSink appends batches with a pipeline. MaxLen with Approx keeps the
// stream bounded when the worker falls behind.
type RedisSink struct {
	client *redis.Client
	stream string
	maxLen int64
}

func NewRedisSink(client *redis.Client, stream string, maxLen int64) *RedisSink {
	if maxLen <= 0 {
		maxLen = 100_000
	}
	return &RedisSink{client: client, stream: stream, maxLen: maxLen}
}

func (s *RedisSink) Publish(ctx context.Context, batch []Event) error {
	pipe := s.client.Pipeline()
	for i := range batch {
		e := &batch[i]
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: s.stream,
			MaxLen: s.maxLen,
			Approx: true,
			Values: map[string]any{
				"ts":         e.At.UnixNano(),
				"tenant":     e.Tenant,
				"method":     e.Method,
				"path":       e.Path,
				"status":     e.Status,
				"latency_us": e.LatencyUs,
				"upstream":   e.Upstream,
			},
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}
