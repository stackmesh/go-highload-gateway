package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/stackmesh/go-highload-gateway/internal/pool"
)

func NewHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       0,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}

func (c *Core) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		c.metrics.Panic()
		if c.log != nil {
			c.log.Error("request panic", "err", rec)
		}
		writeStatic(w, http.StatusInternalServerError)
	}()

	switch r.URL.Path {
	case "/healthz":
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeStatic(w, http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, bodyOK)
		return
	case "/readyz":
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeStatic(w, http.StatusMethodNotAllowed)
			return
		}
		c.ready(w, r)
		return
	case "/metrics":
		c.metrics.Handler().ServeHTTP(w, r)
		return
	default:
		c.proxy(w, r)
	}
}

func (c *Core) ready(w http.ResponseWriter, r *http.Request) {
	snap := c.snap.Load()
	if snap == nil {
		writeJSON(w, http.StatusServiceUnavailable, bodyNotReady)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
	defer cancel()
	dbOK, redisOK := true, true
	if c.db != nil {
		dbOK = c.db.Ping(ctx) == nil
	}
	if c.redis != nil {
		redisOK = c.redis.Ping(ctx) == nil
	}
	code := http.StatusOK
	status := "ok"
	if !dbOK || !redisOK {
		code = http.StatusServiceUnavailable
		status = "degraded"
	}
	body := fmt.Sprintf(`{"status":"%s","postgres":"%s","redis":"%s","routes":%d}`,
		status, up(dbOK), up(redisOK), snap.Routes.Len())
	writeJSON(w, code, []byte(body))
}

func up(ok bool) string {
	if ok {
		return "ok"
	}
	return "down"
}

func (c *Core) proxy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() { drainBody(r.Body) }()

	if !validMethod(r.Method) || !validPath(r.URL.Path) {
		c.observe(admission{}, http.StatusBadRequest, start)
		writeStatic(w, http.StatusBadRequest)
		return
	}

	a, code := c.admit(r.URL.Path, apiKey(r))
	if code != 0 {
		c.finishReject(w, a, code, start)
		return
	}
	if r.ContentLength > c.bodyLimit {
		c.observe(a, http.StatusRequestEntityTooLarge, start)
		writeStatic(w, http.StatusRequestEntityTooLarge)
		return
	}
	a, code = c.arm(a)
	if code != 0 {
		c.observe(a, code, start)
		writeStatic(w, code)
		return
	}

	// A nil ResponseWriter keeps MaxBytesReader from writing its own 413.
	r.Body = http.MaxBytesReader(nil, r.Body, c.bodyLimit)
	result := resultFail
	defer func() { a.done(result) }()

	resp, err := c.forward(r.Context(), a, r.Method, r.URL.Path, r.URL.RawQuery, r.Header, r.Body, r.ContentLength, r.RemoteAddr)
	if err != nil {
		code, result = classifyForwardErr(err)
		c.observe(a, code, start)
		c.emit(a, r.Method, r.URL.Path, code, start)
		writeStatic(w, code)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		result = resultFail
	} else {
		result = resultOK
	}
	copyOutbound(w.Header(), resp.Header)
	w.Header().Set("X-Gateway-Latency-Us", strconv.FormatInt(time.Since(start).Microseconds(), 10))
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(a.key.RPM))
	w.WriteHeader(resp.StatusCode)

	buf := pool.Get()
	defer pool.Put(buf)
	if _, err := io.CopyBuffer(w, resp.Body, *buf); err != nil && resp.StatusCode < 500 {
		result = resultIgnore
	}
	c.observe(a, resp.StatusCode, start)
	c.emit(a, r.Method, r.URL.Path, resp.StatusCode, start)
}

func (c *Core) finishReject(w http.ResponseWriter, a admission, code int, start time.Time) {
	if code == http.StatusTooManyRequests && a.key.RPM > 0 {
		h := w.Header()
		h.Set("Retry-After", "1")
		h.Set("X-RateLimit-Limit", strconv.Itoa(a.key.RPM))
	}
	c.observe(a, code, start)
	writeStatic(w, code)
}

func classifyForwardErr(err error) (int, int) {
	var max *http.MaxBytesError
	if errors.As(err, &max) {
		return http.StatusRequestEntityTooLarge, resultIgnore
	}
	if errors.Is(err, context.Canceled) {
		return 499, resultIgnore
	}
	if timeoutErr(err) {
		return http.StatusGatewayTimeout, resultFail
	}
	return http.StatusBadGateway, resultFail
}

func apiKey(r *http.Request) string {
	if k := r.Header.Get("X-Api-Key"); k != "" {
		return k
	}
	const prefix = "Bearer "
	authz := r.Header.Get("Authorization")
	if len(authz) > len(prefix) && authz[:len(prefix)] == prefix {
		return authz[len(prefix):]
	}
	return ""
}

func drainBody(b io.ReadCloser) {
	if b == nil || b == http.NoBody {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(b, 1<<20))
	_ = b.Close()
}
