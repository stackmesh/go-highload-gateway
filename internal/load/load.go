package load

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Options struct {
	URL         string
	APIKey      string
	Concurrency int
	Method      string
	Timeout     time.Duration
	Warmup      time.Duration
	Duration    time.Duration
}

type Result struct {
	Requests int
	Errors   int
	Elapsed  time.Duration
	samples  []int64
}

func (r Result) RPS() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Requests) / r.Elapsed.Seconds()
}

func (r Result) Percentile(p float64) time.Duration {
	if len(r.samples) == 0 {
		return 0
	}
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	i := int(float64(len(r.samples)-1) * p)
	return time.Duration(r.samples[i]) * time.Microsecond
}

func (r Result) String() string {
	return fmt.Sprintf("requests=%d errors=%d elapsed=%s rps=%.0f p50=%s p95=%s p99=%s",
		r.Requests, r.Errors, r.Elapsed.Truncate(time.Millisecond), r.RPS(),
		r.Percentile(0.50).Truncate(10*time.Microsecond),
		r.Percentile(0.95).Truncate(10*time.Microsecond),
		r.Percentile(0.99).Truncate(10*time.Microsecond))
}

// Run issues keep-alive requests until Duration elapses. Warmup uses the
// same connection pool and is not included in the result.
func Run(ctx context.Context, opt Options) (Result, error) {
	if opt.URL == "" {
		return Result{}, fmt.Errorf("url is required")
	}
	if opt.Concurrency < 1 {
		opt.Concurrency = 1
	}
	if opt.Method == "" {
		opt.Method = http.MethodGet
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 2 * time.Second
	}
	if opt.Duration <= 0 {
		opt.Duration = 10 * time.Second
	}
	tr := &http.Transport{
		Proxy:               nil,
		MaxIdleConns:        opt.Concurrency * 2,
		MaxIdleConnsPerHost: opt.Concurrency * 2,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		IdleConnTimeout:     30 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: opt.Timeout}

	if opt.Warmup > 0 {
		wctx, cancel := context.WithTimeout(ctx, opt.Warmup)
		_, _, _ = drive(wctx, client, opt, false)
		cancel()
	}
	mctx, cancel := context.WithTimeout(ctx, opt.Duration)
	defer cancel()
	start := time.Now()
	ok, bad, samples := drive(mctx, client, opt, true)
	elapsed := time.Since(start)
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return Result{
		Requests: int(ok),
		Errors:   int(bad),
		Elapsed:  elapsed,
		samples:  samples,
	}, nil
}

func drive(ctx context.Context, client *http.Client, opt Options, record bool) (int64, int64, []int64) {
	var ok, bad atomic.Int64
	local := make([][]int64, opt.Concurrency)
	var wg sync.WaitGroup
	for i := 0; i < opt.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var mine []int64
			if record {
				mine = make([]int64, 0, 1024)
			}
			for ctx.Err() == nil {
				reqCtx, cancel := context.WithTimeout(ctx, opt.Timeout)
				req, err := http.NewRequestWithContext(reqCtx, opt.Method, opt.URL, nil)
				if err != nil {
					cancel()
					if ctx.Err() == nil {
						bad.Add(1)
					}
					continue
				}
				if opt.APIKey != "" {
					req.Header.Set("X-Api-Key", opt.APIKey)
				}
				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					cancel()
					if ctx.Err() == nil {
						bad.Add(1)
					}
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				cancel()
				us := time.Since(t0).Microseconds()
				if resp.StatusCode == http.StatusOK {
					ok.Add(1)
					if record {
						mine = append(mine, us)
					}
				} else if ctx.Err() == nil {
					bad.Add(1)
				}
			}
			if record {
				local[i] = mine
			}
		}(i)
	}
	wg.Wait()
	if !record {
		return ok.Load(), bad.Load(), nil
	}
	var n int
	for _, s := range local {
		n += len(s)
	}
	all := make([]int64, 0, n)
	for _, s := range local {
		all = append(all, s...)
	}
	return ok.Load(), bad.Load(), all
}
