package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stackmesh/go-highload-gateway/internal/load"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/v1/echo", "target URL")
	key := flag.String("key", "gw_live_demo", "X-Api-Key value")
	concurrency := flag.Int("c", 128, "concurrent workers")
	duration := flag.Duration("d", 10*time.Second, "measured duration")
	warmup := flag.Duration("warmup", time.Second, "warmup duration")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request timeout")
	method := flag.String("method", "GET", "HTTP method")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	res, err := load.Run(ctx, load.Options{
		URL:         *url,
		APIKey:      *key,
		Concurrency: *concurrency,
		Method:      *method,
		Timeout:     *timeout,
		Warmup:      *warmup,
		Duration:    *duration,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(res.String())
	if res.Requests == 0 || (res.Errors > 0 && float64(res.Errors)/float64(res.Requests+res.Errors) > 0.01) {
		os.Exit(1)
	}
}
