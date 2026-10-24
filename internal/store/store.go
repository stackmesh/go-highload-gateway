package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DemoRPM is high enough that a local load test measures the proxy
	// rather than the quota. 6_000_000 per minute is 100_000 per second.
	DemoRPM = 6_000_000
	// LimitedRPM is the key used to demonstrate 429 responses.
	LimitedRPM = 60

	echoRoute   = "echo"
	echoPrefix  = "/v1/echo"
	demoTenant  = "demo"
	limitTenant = "limited"
)

type APIKey struct {
	ID     uint64
	Hash   [32]byte
	Tenant string
	RPM    int
}

type Route struct {
	Name     string
	Prefix   string
	Upstream string
	Timeout  time.Duration
}

type Log struct {
	At        time.Time
	Tenant    string
	Method    string
	Path      string
	Status    int
	LatencyUs int64
	Upstream  string
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url, app string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 15 * time.Minute
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if app != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = app
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// OpenWithRetry waits up to 30s for Postgres. Compose starts the gateway
// after the healthcheck, and a local `make up` can still race the first ping.
func OpenWithRetry(ctx context.Context, url, app string) (*Store, error) {
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for {
		s, err := Open(ctx, url, app)
		if err == nil {
			return s, nil
		}
		last = err
		if ctx.Err() != nil || time.Now().After(deadline) {
			return nil, fmt.Errorf("postgres: %w", last)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func ms(n int) time.Duration {
	return time.Duration(n) * time.Millisecond
}
