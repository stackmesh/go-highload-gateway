package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stackmesh/go-highload-gateway/internal/store"
)

func decodeMessage(msg redis.XMessage) (store.Log, bool) {
	ts, err := strconv.ParseInt(field(msg.Values, "ts"), 10, 64)
	if err != nil {
		return store.Log{}, false
	}
	status, err := strconv.Atoi(field(msg.Values, "status"))
	if err != nil {
		return store.Log{}, false
	}
	latency, err := strconv.ParseInt(field(msg.Values, "latency_us"), 10, 64)
	if err != nil {
		return store.Log{}, false
	}
	return store.Log{
		At:        time.Unix(0, ts).UTC(),
		Tenant:    clip(field(msg.Values, "tenant"), 128),
		Method:    clip(field(msg.Values, "method"), 16),
		Path:      clip(field(msg.Values, "path"), 512),
		Status:    status,
		LatencyUs: latency,
		Upstream:  clip(field(msg.Values, "upstream"), 256),
	}, true
}

func field(v map[string]any, key string) string {
	raw, ok := v[key]
	if !ok || raw == nil {
		return ""
	}
	switch t := raw.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
