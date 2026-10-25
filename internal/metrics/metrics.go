package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics uses a private registry so tests can construct more than one gateway.
type Metrics struct {
	reg             *prometheus.Registry
	requests        *prometheus.CounterVec
	latency         *prometheus.HistogramVec
	limited         prometheus.Counter
	breakerRejected prometheus.Counter
	dropped         prometheus.Counter
	published       prometheus.Counter
	failed          prometheus.Counter
	panics          prometheus.Counter
	handler         http.Handler
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Requests by route name and HTTP status.",
		}, []string{"route", "code"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "End-to-end handler latency.",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}, []string{"route"}),
		limited: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_rate_limited_total",
			Help: "Requests rejected by the local token bucket.",
		}),
		breakerRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_breaker_rejected_total",
			Help: "Requests rejected because the upstream circuit was open.",
		}),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_events_dropped_total",
			Help: "Audit events dropped because the publish buffer was full.",
		}),
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_events_published_total",
			Help: "Audit events appended to Redis.",
		}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_events_failed_total",
			Help: "Audit batches that failed to append.",
		}),
		panics: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_panics_total",
			Help: "Recovered handler panics.",
		}),
	}
	reg.MustRegister(
		m.requests,
		m.latency,
		m.limited,
		m.breakerRejected,
		m.dropped,
		m.published,
		m.failed,
		m.panics,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m.handler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	return m
}

func (m *Metrics) Handler() http.Handler { return m.handler }

func (m *Metrics) Observe(route string, status int, d time.Duration) {
	if route == "" {
		route = "none"
	}
	code := strconv.Itoa(status)
	m.requests.WithLabelValues(route, code).Inc()
	m.latency.WithLabelValues(route).Observe(d.Seconds())
}

func (m *Metrics) Limited()         { m.limited.Inc() }
func (m *Metrics) BreakerRejected() { m.breakerRejected.Inc() }
func (m *Metrics) Dropped()         { m.dropped.Inc() }
func (m *Metrics) Published(n int)  { m.published.Add(float64(n)) }
func (m *Metrics) Failed()          { m.failed.Inc() }
func (m *Metrics) Panic()           { m.panics.Inc() }
