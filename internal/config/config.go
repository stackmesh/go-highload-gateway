package config

import (
	"os"
	"strconv"
	"time"
)

// Config is the process configuration. Every field has a default so the
// binaries run against a local Postgres and Redis without an env file.
type Config struct {
	HTTPAddr         string
	GRPCAddr         string
	PprofAddr        string
	DatabaseURL      string
	RedisAddr        string
	RedisStream      string
	RedisGroup       string
	UpstreamURL      string
	UpstreamAddr     string
	DemoAPIKey       string
	LimitedAPIKey    string
	LogLevel         string
	RefreshInterval  time.Duration
	BreakerCooldown  time.Duration
	BreakerThreshold int
	BodyLimit        int64
	PublishBuffer    int
	StreamMaxLen     int64
	ShutdownTimeout  time.Duration
	SweepAfter       time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:         getenv("HTTP_ADDR", ":8080"),
		GRPCAddr:         getenv("GRPC_ADDR", ":9090"),
		PprofAddr:        os.Getenv("PPROF_ADDR"),
		DatabaseURL:      getenv("DATABASE_URL", "postgres://gateway:gateway@127.0.0.1:5432/gateway?sslmode=disable"),
		RedisAddr:        getenv("REDIS_ADDR", "127.0.0.1:6379"),
		RedisStream:      getenv("REDIS_STREAM", "gateway.requests"),
		RedisGroup:       getenv("REDIS_GROUP", "audit-workers"),
		UpstreamURL:      getenv("UPSTREAM_URL", "http://127.0.0.1:8081"),
		UpstreamAddr:     getenv("UPSTREAM_ADDR", ":8081"),
		DemoAPIKey:       getenv("DEMO_API_KEY", "gw_live_demo"),
		LimitedAPIKey:    getenv("LIMITED_API_KEY", "gw_live_limited"),
		LogLevel:         getenv("LOG_LEVEL", "info"),
		RefreshInterval:  getdur("REFRESH_INTERVAL", 5*time.Second),
		BreakerCooldown:  getdur("BREAKER_COOLDOWN", 5*time.Second),
		BreakerThreshold: getint("BREAKER_THRESHOLD", 5),
		BodyLimit:        getint64("BODY_LIMIT", 1<<20),
		PublishBuffer:    getint("PUBLISH_BUFFER", 8192),
		StreamMaxLen:     getint64("STREAM_MAXLEN", 100_000),
		ShutdownTimeout:  getdur("SHUTDOWN_TIMEOUT", 10*time.Second),
		SweepAfter:       getdur("SWEEP_AFTER", 10*time.Minute),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getdur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func getint(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getint64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}
