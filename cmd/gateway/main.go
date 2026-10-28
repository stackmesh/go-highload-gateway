package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	gatewayv1 "github.com/stackmesh/go-highload-gateway/api/gen/gateway/v1"
	"github.com/stackmesh/go-highload-gateway/internal/auth"
	"github.com/stackmesh/go-highload-gateway/internal/config"
	"github.com/stackmesh/go-highload-gateway/internal/gateway"
	"github.com/stackmesh/go-highload-gateway/internal/metrics"
	"github.com/stackmesh/go-highload-gateway/internal/proxy"
	"github.com/stackmesh/go-highload-gateway/internal/store"
	"github.com/stackmesh/go-highload-gateway/internal/stream"
)

func main() {
	cfg := config.Load()
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	db, err := store.OpenWithRetry(ctx, cfg.DatabaseURL, "gateway")
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		return err
	}
	if err := db.Seed(ctx, auth.Sum(cfg.DemoAPIKey), auth.Sum(cfg.LimitedAPIKey), cfg.UpstreamURL, 2000); err != nil {
		return err
	}

	rdb := stream.NewRedis(cfg.RedisAddr)
	defer rdb.Close()
	if err := stream.Wait(ctx, rdb); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	met := metrics.New()
	pub := stream.New(cfg.PublishBuffer, stream.NewRedisSink(rdb, cfg.RedisStream, cfg.StreamMaxLen), stream.Hooks{
		Dropped:   met.Dropped,
		Published: met.Published,
		Failed:    met.Failed,
	})
	pubCtx, pubCancel := context.WithCancel(context.Background())
	go pub.Run(pubCtx)

	gates := proxy.NewRegistry(cfg.BreakerThreshold, cfg.BreakerCooldown)
	core := gateway.New(gateway.Options{
		Log:       log,
		Publisher: pub,
		Metrics:   met,
		BodyLimit: cfg.BodyLimit,
		DB:        db,
		Redis:     redisPinger{c: rdb},
	})
	if err := reload(ctx, db, core, gates); err != nil {
		pubCancel()
		<-pub.Done()
		return err
	}

	go func() {
		t := time.NewTicker(cfg.RefreshInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				err := reload(rctx, db, core, gates)
				cancel()
				if err != nil && ctx.Err() == nil {
					log.Error("refresh", "err", err)
					continue
				}
				core.Sweep(time.Now().Add(-cfg.SweepAfter))
			}
		}
	}()

	if cfg.PprofAddr != "" {
		go func() {
			log.Info("pprof", "addr", cfg.PprofAddr)
			if err := http.ListenAndServe(cfg.PprofAddr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("pprof", "err", err)
			}
		}()
	}

	httpLn, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		pubCancel()
		<-pub.Done()
		return err
	}
	grpcLn, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		httpLn.Close()
		pubCancel()
		<-pub.Done()
		return err
	}

	httpSrv := gateway.NewHTTPServer(cfg.HTTPAddr, core)
	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(int(cfg.BodyLimit)),
		grpc.MaxSendMsgSize(int(cfg.BodyLimit)),
	)
	gatewayv1.RegisterGatewayServer(grpcSrv, gateway.NewGRPC(core))
	reflection.Register(grpcSrv)

	errCh := make(chan error, 2)
	go func() { errCh <- httpSrv.Serve(httpLn) }()
	go func() { errCh <- grpcSrv.Serve(grpcLn) }()
	log.Info("listening", "http", httpLn.Addr().String(), "grpc", grpcLn.Addr().String())

	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr = err
		}
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http shutdown", "err", err)
	}
	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutCtx.Done():
		grpcSrv.Stop()
	}
	pubCancel()
	<-pub.Done()
	return serveErr
}

func reload(ctx context.Context, db *store.Store, core *gateway.Core, gates *proxy.Registry) error {
	keys, routes, err := db.Load(ctx)
	if err != nil {
		return err
	}
	core.SetSnapshot(gateway.BuildSnapshot(keys, routes, gates))
	return nil
}

type redisPinger struct{ c *redis.Client }

func (p redisPinger) Ping(ctx context.Context) error { return p.c.Ping(ctx).Err() }

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
