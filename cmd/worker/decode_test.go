package main

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestDecodeMessage(t *testing.T) {
	row, ok := decodeMessage(redis.XMessage{
		ID: "1-0",
		Values: map[string]any{
			"ts":         "100",
			"tenant":     "demo",
			"method":     "GET",
			"path":       "/v1/echo",
			"status":     "200",
			"latency_us": "15",
			"upstream":   "http://upstream:8081",
		},
	})
	if !ok {
		t.Fatal("expected ok")
	}
	if row.Status != 200 || row.Tenant != "demo" || row.Path != "/v1/echo" || row.LatencyUs != 15 {
		t.Fatalf("%+v", row)
	}
	if !row.At.Equal(time.Unix(0, 100).UTC()) {
		t.Fatalf("at %s", row.At)
	}
	if _, ok := decodeMessage(redis.XMessage{Values: map[string]any{"ts": "nope"}}); ok {
		t.Fatal("expected poison message")
	}
}
