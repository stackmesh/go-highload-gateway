package gateway

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	gatewayv1 "github.com/stackmesh/go-highload-gateway/api/gen/gateway/v1"
	"github.com/stackmesh/go-highload-gateway/internal/auth"
	"github.com/stackmesh/go-highload-gateway/internal/catalog"
	"github.com/stackmesh/go-highload-gateway/internal/load"
	"github.com/stackmesh/go-highload-gateway/internal/proxy"
	"github.com/stackmesh/go-highload-gateway/internal/router"
	"github.com/stackmesh/go-highload-gateway/internal/stream"
)

func TestJoinURL(t *testing.T) {
	got, err := joinURL("http://upstream:8081", "/v1/echo", "q=1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://upstream:8081/v1/echo?q=1" {
		t.Fatalf("got %s", got)
	}
	if _, err := joinURL("upstream", "/v1", ""); err == nil {
		t.Fatal("expected relative upstream to fail")
	}
}

func TestProxyAdmissionAndForward(t *testing.T) {
	var sawTenant, sawCred atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Tenant") == "demo" {
			sawTenant.Store(true)
		}
		if r.Header.Get("X-Api-Key") != "" || r.Header.Get("Authorization") != "" {
			sawCred.Store(true)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)

	core, events := testCore(t, upstream.URL, 6_000_000, proxy.NewBreaker(5, time.Second))
	srv := httptest.NewServer(core)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, res)
	if res.StatusCode != 200 || string(body) != `{"status":"ok"}` {
		t.Fatalf("health %d %s", res.StatusCode, body)
	}

	res, err = http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	body = readAll(t, res)
	if res.StatusCode != 200 || !strings.Contains(string(body), `"routes":1`) {
		t.Fatalf("ready %d %s", res.StatusCode, body)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/echo", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body = readAll(t, res)
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing key: %d %s", res.StatusCode, body)
	}

	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/v1/echo?x=1", nil)
	req.Header.Set("Authorization", "Bearer gw_live_demo")
	req.Header.Set("X-Api-Key", "gw_live_demo")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body = readAll(t, res)
	if res.StatusCode != 200 || string(body) != `{"ok":true}` {
		t.Fatalf("proxy %d %s", res.StatusCode, body)
	}
	if !sawTenant.Load() || sawCred.Load() {
		t.Fatalf("tenant=%v cred_forwarded=%v", sawTenant.Load(), sawCred.Load())
	}
	select {
	case ev := <-events:
		if ev.Status != 200 || ev.Tenant != "demo" || ev.Path != "/v1/echo" {
			t.Fatalf("event %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("audit event was not published")
	}

	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/missing", nil)
	req.Header.Set("X-Api-Key", "gw_live_demo")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("missing route %d", res.StatusCode)
	}
	res.Body.Close()

	res, err = http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body = readAll(t, res)
	if !strings.Contains(string(body), "gateway_requests_total") {
		t.Fatalf("metrics missing counter: %s", body)
	}
}

func TestRateLimit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	core, _ := testCore(t, upstream.URL, 6_000_000, proxy.NewBreaker(5, time.Second))
	srv := httptest.NewServer(core)
	t.Cleanup(srv.Close)

	ok := 0
	limited := 0
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/echo", nil)
		req.Header.Set("X-Api-Key", "gw_live_limited")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		switch res.StatusCode {
		case 200:
			ok++
		case http.StatusTooManyRequests:
			limited++
			if res.Header.Get("Retry-After") != "1" {
				t.Fatalf("retry-after %q", res.Header.Get("Retry-After"))
			}
		default:
			t.Fatalf("status %d", res.StatusCode)
		}
	}
	if ok != 2 || limited != 3 {
		t.Fatalf("ok=%d limited=%d", ok, limited)
	}
}

func TestBreakerOpens(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	t.Cleanup(upstream.Close)
	core, _ := testCore(t, upstream.URL, 6_000_000, proxy.NewBreaker(1, time.Hour))
	srv := httptest.NewServer(core)
	t.Cleanup(srv.Close)

	for i, want := range []int{500, 503, 503} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/echo", nil)
		req.Header.Set("X-Api-Key", "gw_live_demo")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("call %d status %d want %d", i, res.StatusCode, want)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("upstream hits %d, breaker should have stopped the rest", hits.Load())
	}
}

func TestGRPCProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	core, _ := testCore(t, upstream.URL, 6_000_000, proxy.NewBreaker(5, time.Second))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	gatewayv1.RegisterGatewayServer(gs, NewGRPC(core))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := gatewayv1.NewGatewayClient(conn)
	resp, err := client.Proxy(context.Background(), &gatewayv1.ProxyRequest{
		Method: http.MethodGet,
		Path:   "/v1/echo",
		ApiKey: "gw_live_demo",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != 200 || string(resp.GetBody()) != `{"ok":true}` {
		t.Fatalf("grpc status %d body %s", resp.GetStatus(), resp.GetBody())
	}
}

func TestConcurrentProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	core, _ := testCore(t, upstream.URL, 6_000_000, proxy.NewBreaker(5, time.Second))
	srv := httptest.NewServer(core)
	t.Cleanup(srv.Close)

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 40; n++ {
				req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/echo", nil)
				req.Header.Set("X-Api-Key", "gw_live_demo")
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode != 200 {
					select {
					case errCh <- errStatus(res.StatusCode):
					default:
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestSustainsFiveThousandRPS(t *testing.T) {
	if os.Getenv("PEAK") == "" {
		t.Skip("set PEAK=1 to run the throughput assertion")
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	core, _ := testCore(t, upstream.URL, 6_000_000, proxy.NewBreaker(50, time.Second))
	srv := httptest.NewServer(core)
	t.Cleanup(srv.Close)

	res, err := load.Run(context.Background(), load.Options{
		URL:         srv.URL + "/v1/echo",
		APIKey:      "gw_live_demo",
		Concurrency: runtime.NumCPU() * 8,
		Warmup:      time.Second,
		Duration:    3 * time.Second,
		Timeout:     2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(res.String())
	if res.Errors > 0 {
		t.Fatalf("errors: %d", res.Errors)
	}
	if res.RPS() < 5000 {
		t.Fatalf("sustained %.0f rps, want at least 5000", res.RPS())
	}
}

func BenchmarkProxy(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	b.Cleanup(upstream.Close)
	core, _ := testCore(b, upstream.URL, 6_000_000, proxy.NewBreaker(1000, time.Second))
	srv := httptest.NewServer(core)
	b.Cleanup(srv.Close)

	tr := srv.Client().Transport.(*http.Transport)
	tr.MaxIdleConns = 256
	tr.MaxIdleConnsPerHost = 256
	tr.DisableCompression = true
	client := srv.Client()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/echo", nil)
	req.Header.Set("X-Api-Key", "gw_live_demo")

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := client.Do(req.Clone(context.Background()))
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != 200 {
				b.Fatalf("status %d", res.StatusCode)
			}
		}
	})
}

func testCore(tb testing.TB, upstream string, demoRPM int, br *proxy.Breaker) (*Core, <-chan stream.Event) {
	tb.Helper()
	events := make(chan stream.Event, 32)
	pub := stream.New(32, sinkFunc(func(batch []stream.Event) error {
		for _, e := range batch {
			select {
			case events <- e:
			default:
			}
		}
		return nil
	}), stream.Hooks{})
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(func() {
		cancel()
		select {
		case <-pub.Done():
		case <-time.After(time.Second):
		}
	})
	go pub.Run(ctx)

	snap := &catalog.Snapshot{
		Keys: map[[32]byte]catalog.Key{
			auth.Sum("gw_live_demo"):    {ID: 1, Tenant: "demo", RPM: demoRPM},
			auth.Sum("gw_live_limited"): {ID: 2, Tenant: "limited", RPM: 60},
		},
		Routes: router.New([]router.Route{{
			Name:     "echo",
			Prefix:   "/v1/echo",
			Upstream: upstream,
			Timeout:  time.Second,
			Breaker:  br,
		}}),
	}
	c := New(Options{Publisher: pub, BodyLimit: 1 << 20})
	c.SetSnapshot(snap)
	return c, events
}

type sinkFunc func(batch []stream.Event) error

func (f sinkFunc) Publish(ctx context.Context, batch []stream.Event) error { return f(batch) }

func readAll(t *testing.T, res *http.Response) []byte {
	t.Helper()
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type errStatus int

func (e errStatus) Error() string {
	return "unexpected status " + http.StatusText(int(e))
}
