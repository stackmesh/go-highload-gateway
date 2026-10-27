package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/stackmesh/go-highload-gateway/internal/auth"
	"github.com/stackmesh/go-highload-gateway/internal/catalog"
	"github.com/stackmesh/go-highload-gateway/internal/metrics"
	"github.com/stackmesh/go-highload-gateway/internal/proxy"
	"github.com/stackmesh/go-highload-gateway/internal/ratelimit"
	"github.com/stackmesh/go-highload-gateway/internal/router"
	"github.com/stackmesh/go-highload-gateway/internal/store"
	"github.com/stackmesh/go-highload-gateway/internal/stream"
)

const (
	resultOK = iota
	resultFail
	resultIgnore
)

// Pinger is satisfied by the Postgres store and by a small Redis adapter.
type Pinger interface {
	Ping(context.Context) error
}

type Options struct {
	Log       *slog.Logger
	Client    *http.Client
	Limiter   *ratelimit.Limiter
	Publisher *stream.Publisher
	Metrics   *metrics.Metrics
	BodyLimit int64
	DB        Pinger
	Redis     Pinger
}

// Core is the data plane shared by the HTTP server and the gRPC service.
type Core struct {
	log       *slog.Logger
	snap      atomic.Pointer[catalog.Snapshot]
	limit     *ratelimit.Limiter
	pub       *stream.Publisher
	client    *http.Client
	metrics   *metrics.Metrics
	bodyLimit int64
	db        Pinger
	redis     Pinger
}

func New(opt Options) *Core {
	if opt.Client == nil {
		opt.Client = proxy.NewClient()
	}
	if opt.Limiter == nil {
		opt.Limiter = ratelimit.New()
	}
	if opt.Metrics == nil {
		opt.Metrics = metrics.New()
	}
	if opt.BodyLimit <= 0 {
		opt.BodyLimit = 1 << 20
	}
	return &Core{
		log:       opt.Log,
		limit:     opt.Limiter,
		pub:       opt.Publisher,
		client:    opt.Client,
		metrics:   opt.Metrics,
		bodyLimit: opt.BodyLimit,
		db:        opt.DB,
		redis:     opt.Redis,
	}
}

func (c *Core) SetSnapshot(s *catalog.Snapshot) { c.snap.Store(s) }

func (c *Core) Sweep(before time.Time) { c.limit.Sweep(before.UnixNano()) }

func (c *Core) Metrics() *metrics.Metrics { return c.metrics }

// BuildSnapshot copies control-plane rows into an immutable snapshot and
// attaches the long-lived breaker for each upstream.
func BuildSnapshot(keys []store.APIKey, routes []store.Route, gates *proxy.Registry) *catalog.Snapshot {
	m := make(map[[32]byte]catalog.Key, len(keys))
	for _, k := range keys {
		m[k.Hash] = catalog.Key{ID: k.ID, Tenant: k.Tenant, RPM: k.RPM}
	}
	rr := make([]router.Route, 0, len(routes))
	for _, r := range routes {
		rr = append(rr, router.Route{
			Name:     r.Name,
			Prefix:   r.Prefix,
			Upstream: r.Upstream,
			Timeout:  r.Timeout,
			Breaker:  gates.Get(r.Upstream),
		})
	}
	return &catalog.Snapshot{Keys: m, Routes: router.New(rr)}
}

type admission struct {
	key   catalog.Key
	route router.Route
	armed bool
}

func (a admission) done(result int) {
	if !a.armed || a.route.Breaker == nil {
		return
	}
	switch result {
	case resultOK:
		a.route.Breaker.Success()
	case resultFail:
		a.route.Breaker.Failure()
	default:
		a.route.Breaker.Ignore()
	}
}

func (a admission) routeName() string {
	if a.route.Name == "" {
		return "none"
	}
	return a.route.Name
}

// admit authenticates, matches a route, and applies the local rate limit.
// The circuit breaker is armed by the caller after the body limit check so
// a rejected body does not consume a half-open probe.
func (c *Core) admit(path, token string) (admission, int) {
	snap := c.snap.Load()
	if snap == nil {
		return admission{}, http.StatusServiceUnavailable
	}
	if token == "" {
		return admission{}, http.StatusUnauthorized
	}
	key, ok := snap.Keys[auth.Sum(token)]
	if !ok {
		return admission{}, http.StatusUnauthorized
	}
	route, ok := snap.Routes.Match(path)
	if !ok {
		return admission{key: key}, http.StatusNotFound
	}
	a := admission{key: key, route: route}
	if !c.limit.Allow(key.ID, key.RPM) {
		c.metrics.Limited()
		return a, http.StatusTooManyRequests
	}
	return a, 0
}

func (c *Core) arm(a admission) (admission, int) {
	if a.route.Breaker != nil && !a.route.Breaker.Allow() {
		c.metrics.BreakerRejected()
		return a, http.StatusServiceUnavailable
	}
	a.armed = true
	return a, 0
}

func (c *Core) observe(a admission, status int, start time.Time) {
	c.metrics.Observe(a.routeName(), status, time.Since(start))
}

func (c *Core) emit(a admission, method, path string, status int, start time.Time) {
	if c.pub == nil || status == 0 {
		return
	}
	end := time.Now()
	c.pub.Emit(stream.Event{
		At:        end,
		Tenant:    a.key.Tenant,
		Method:    method,
		Path:      clip(path, 512),
		Status:    status,
		LatencyUs: end.Sub(start).Microseconds(),
		Upstream:  a.route.Upstream,
	})
}
