//go:build integration

package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stackmesh/go-highload-gateway/internal/auth"
)

func TestRoundTrip(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := Open(ctx, url, "gateway-test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Seed(ctx, auth.Sum("gw_live_demo"), auth.Sum("gw_live_limited"), "http://127.0.0.1:8081", 2000); err != nil {
		t.Fatal(err)
	}
	keys, routes, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) < 2 || len(routes) < 1 {
		t.Fatalf("keys=%d routes=%d", len(keys), len(routes))
	}
	err = s.InsertLogs(ctx, []Log{{
		At:        time.Now().UTC(),
		Tenant:    "demo",
		Method:    "GET",
		Path:      "/v1/echo",
		Status:    200,
		LatencyUs: 100,
		Upstream:  "http://127.0.0.1:8081",
	}})
	if err != nil {
		t.Fatal(err)
	}
}
