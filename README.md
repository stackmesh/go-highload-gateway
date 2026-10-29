# go-highload-gateway

A concurrent, distributed API gateway in Go. It sustains peak volumes of 5,000+ RPS with an allocation-light data plane.

Clients call HTTP or gRPC. The gateway authenticates the API key, matches a route, applies a local token bucket and a per-upstream circuit breaker, then proxies to the upstream with a keep-alive pool. Audit events are queued to Redis Streams. A separate worker batch-inserts them into PostgreSQL. A request returns without waiting on either database.

**Stack:** Go, gRPC, Protocol Buffers, PostgreSQL (`pgx`), Redis Streams, Docker.

## Architecture

A request is admitted from memory, proxied, and returned. Redis and Postgres sit off that path.

```mermaid
flowchart TB
  client([Client])

  subgraph hot ["Response path"]
    direction TB
    key["1 · API key, SHA-256"]
    route["2 · Longest prefix"]
    bucket["3 · Token bucket"]
    breaker["4 · Circuit breaker"]
    proxy["5 · Keep-alive proxy"]
    key --> route --> bucket --> breaker --> proxy
  end

  upstream([Upstream])

  subgraph later ["After the response"]
    direction LR
    buf["Event buffer"]
    redis[("Redis Stream")]
    worker["Audit worker"]
    pg[("PostgreSQL")]
    buf --> redis --> worker --> pg
  end

  client -->|"HTTP :8080 or gRPC :9090"| key
  proxy --> upstream
  proxy -.->|"enqueue, do not wait"| buf
  pg -.->|"snapshot every 5s"| key

  classDef step fill:#eef3f8,stroke:#3d5a73,color:#1c2833
  classDef side fill:#f8f1e7,stroke:#8a6232,color:#3d2b14
  classDef edge fill:#f4f6f5,stroke:#5c6b66,color:#1c2833
  class key,route,bucket,breaker,proxy step
  class buf,redis,worker,pg side
  class client,upstream edge
```

Solid arrows block the response. The dotted arrow into the buffer does not: a full channel drops the event, increments `gateway_events_dropped_total`, and the client still gets an answer. The other dotted arrow is the control plane, a snapshot swap, not a per-request read.

- **Data plane.** `cmd/gateway` loads that snapshot through `atomic.Pointer`. Admission is a hash lookup, a prefix scan, and a shard lock. The HTTP body is copied with a pooled 32KiB buffer. gRPC uses the same admission path and returns a protobuf message.
- **Control plane.** Routes and key hashes live in Postgres. A refresh loop swaps the snapshot every 5 seconds. The audit worker (`cmd/worker`) consumes the stream with a consumer group and writes `request_log` with `COPY`.

Replicas are stateless. Put a load balancer in front and point them at the same Postgres and Redis. Each replica enforces RPM on its own, so two replicas allow twice the configured rate. That keeps Redis off the hot path.

## Performance

Admission stays allocation-free once a key has a bucket. Measured with `go test -bench`:

| Path | Time | Allocations |
| --- | ---: | --- |
| Route match | 33 ns | 0 B, 0 allocs |
| Token bucket | 47 ns | 0 B, 0 allocs |
| API-key SHA-256 | 185 ns | 0 B, 0 allocs |
| 32KiB buffer get/put | 10 ns | 0 B, 0 allocs |

End-to-end, `make peak` drives `GET /v1/echo` through the HTTP gateway to a local upstream. The generator and the gateway share the process. On an Intel i7-10700K (16 threads), Go 1.27:

```text
requests=232203 errors=0 elapsed=3.002s rps=77335 p50=1.28ms p95=4.19ms p99=6.02ms
```

What keeps that path cheap:

- API keys are stored as raw SHA-256 (`[32]byte` map keys). The hasher and digest buffer are pooled.
- The route table is an immutable slice sorted by prefix length. A linear scan matches control-plane route counts.
- The token bucket is 64 shards. Refill is `rpm/60` tokens per second, burst is two seconds of that rate.
- Upstream connections are pooled (`MaxIdleConnsPerHost` 1024). The proxy returns upstream redirects to the client.
- Error responses are static byte slices.
- Audit events go into an 8192-slot channel. A full channel increments `gateway_events_dropped_total` and the request still returns.
- The publisher pipeline-appends to Redis and caps the stream with `MAXLEN ~ 100000`.
- The worker inserts with `COPY`, then `XACK`s. Idle pending messages are reclaimed with `XAUTOCLAIM`.

`BenchmarkProxy` is about 16 µs/op and ~185 allocs/op. That number includes the HTTP client, the gateway, and the upstream server in one process. The admission checks above stay at 0 allocs; the remaining allocations are the `net/http` stacks.

## Run

```bash
docker compose up -d --build
curl -s -H 'X-Api-Key: gw_live_demo' http://127.0.0.1:8080/v1/echo
```

The demo key `gw_live_demo` is seeded at 6,000,000 rpm (100,000 rps) so a load test measures the proxy. `gw_live_limited` is seeded at 60 rpm, which is a burst of 2 and then one token per second:

```bash
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "%{http_code}\n" -H 'X-Api-Key: gw_live_limited' http://127.0.0.1:8080/v1/echo
done
```

Load from the host, or with the compose profile:

```bash
make load
docker compose --profile load run --rm loadgen
```

gRPC uses the same pipeline. Server reflection is enabled:

```bash
grpcurl -plaintext -d '{"method":"GET","path":"/v1/echo","api_key":"gw_live_demo"}' \
  localhost:9090 gateway.v1.Gateway/Proxy
```

After traffic, the worker's rows are in Postgres:

```bash
docker compose exec postgres \
  psql -U gateway -d gateway -c 'SELECT status, count(*) FROM request_log GROUP BY 1'
```

Add a route. The gateway picks it up on the next refresh:

```sql
INSERT INTO routes (name, path_prefix, upstream, timeout_ms)
VALUES ('billing', '/v1/billing', 'http://billing:8080', 1500);
```

API keys are the raw SHA-256 of the token:

```bash
python3 -c 'import hashlib; print(hashlib.sha256(b"a-new-token").hexdigest())'
```

```sql
INSERT INTO api_keys (key_hash, tenant, rpm)
VALUES (decode('<hex from above>', 'hex'), 'acme', 6000);
```

Changing `rpm` in SQL survives restarts. The demo route's upstream is upserted from `UPSTREAM_URL` on boot.

`/healthz` is process liveness. `/readyz` pings Postgres and Redis. `/metrics` is Prometheus. Those three paths are reserved.

Without Docker, the same tests run locally:

```bash
go test ./...
make bench
make peak
```

`make peak` asserts at least 5,000 RPS against the in-process data plane.

## Configuration

| Variable | Default |
| --- | --- |
| `HTTP_ADDR` | `:8080` |
| `GRPC_ADDR` | `:9090` |
| `PPROF_ADDR` | empty (off). Compose sets `:6060` |
| `DATABASE_URL` | `postgres://gateway:gateway@127.0.0.1:5432/gateway?sslmode=disable` |
| `REDIS_ADDR` | `127.0.0.1:6379` |
| `REDIS_STREAM` | `gateway.requests` |
| `REDIS_GROUP` | `audit-workers` |
| `UPSTREAM_URL` | `http://127.0.0.1:8081` |
| `DEMO_API_KEY` | `gw_live_demo` |
| `LIMITED_API_KEY` | `gw_live_limited` |
| `BODY_LIMIT` | `1048576` |
| `REFRESH_INTERVAL` | `5s` |
| `BREAKER_THRESHOLD` | `5` |
| `BREAKER_COOLDOWN` | `5s` |
| `PUBLISH_BUFFER` | `8192` |
| `STREAM_MAXLEN` | `100000` |
| `LOG_LEVEL` | `info` |

pprof is unauthenticated. Leave `PPROF_ADDR` empty on any interface you do not trust. The demo keys are for local runs. Tokens are hashed with SHA-256, which is appropriate for high-entropy keys and the wrong function for passwords.

Audit delivery is at-least-once. A crash between `COPY` and `XACK` can insert the same batch twice.

## Layout

```text
cmd/gateway     HTTP and gRPC data plane
cmd/worker      Redis Streams consumer, Postgres COPY
cmd/upstream    tiny echo upstream for local runs
cmd/loadgen     keep-alive load generator
api/proto       gateway.v1.Gateway/Proxy
internal/gateway    admission, proxy, gRPC adapter
internal/ratelimit  sharded token bucket
internal/router     longest-prefix route table
internal/proxy      circuit breaker and connection pool
internal/stream     batched publisher
internal/store      pgx pool, migrations, COPY
```

## Pinning this repository

Suggested description:

> A concurrent, distributed API Gateway microservice capable of sustaining peak volumes of 5,000+ RPS with highly optimized memory allocations.

Topics: `golang` `grpc` `protobuf` `postgresql` `redis` `api-gateway` `docker` `high-performance`

Replace `github.com/stackmesh/go-highload-gateway` in `go.mod` and the imports if you publish the module under a different GitHub account.
