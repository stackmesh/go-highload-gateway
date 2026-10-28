package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stackmesh/go-highload-gateway/internal/config"
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
	db, err := store.OpenWithRetry(ctx, cfg.DatabaseURL, "gateway-worker")
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		return err
	}

	rdb := stream.NewRedis(cfg.RedisAddr)
	defer rdb.Close()
	if err := stream.Wait(ctx, rdb); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	if err := ensureGroup(ctx, rdb, cfg.RedisStream, cfg.RedisGroup); err != nil {
		return err
	}

	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	consumer := host + "-" + strconv.Itoa(os.Getpid())
	log.Info("consuming", "stream", cfg.RedisStream, "group", cfg.RedisGroup, "consumer", consumer)

	var inserted int64
	lastClaim := time.Now()
	lastLog := time.Now()
	for ctx.Err() == nil {
		if time.Since(lastClaim) > 10*time.Second {
			lastClaim = time.Now()
			n, err := claim(ctx, rdb, db, cfg, consumer, log)
			if err != nil && ctx.Err() == nil {
				log.Error("autoclaim", "err", err)
			}
			inserted += int64(n)
		}

		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    cfg.RedisGroup,
			Consumer: consumer,
			Streams:  []string{cfg.RedisStream, ">"},
			Count:    500,
			Block:    2 * time.Second,
		}).Result()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, redis.Nil) {
				continue
			}
			log.Error("read", "err", err)
			continue
		}
		n, err := persist(ctx, rdb, db, cfg.RedisStream, cfg.RedisGroup, streams, log)
		if err != nil {
			log.Error("persist", "err", err)
			continue
		}
		inserted += int64(n)
		if n > 0 && time.Since(lastLog) > time.Second {
			log.Info("flushed", "rows", n, "total", inserted)
			lastLog = time.Now()
		}
	}
	return nil
}

func ensureGroup(ctx context.Context, rdb *redis.Client, streamName, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, streamName, group, "0").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("create group: %w", err)
}

func claim(ctx context.Context, rdb *redis.Client, db *store.Store, cfg config.Config, consumer string, log *slog.Logger) (int, error) {
	msgs, _, err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   cfg.RedisStream,
		Group:    cfg.RedisGroup,
		Consumer: consumer,
		MinIdle:  time.Minute,
		Start:    "0-0",
		Count:    200,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	return persist(ctx, rdb, db, cfg.RedisStream, cfg.RedisGroup, []redis.XStream{{
		Stream:   cfg.RedisStream,
		Messages: msgs,
	}}, log)
}

func persist(ctx context.Context, rdb *redis.Client, db *store.Store, streamName, group string, streams []redis.XStream, log *slog.Logger) (int, error) {
	var logs []store.Log
	var ids []string
	for _, st := range streams {
		for _, msg := range st.Messages {
			ids = append(ids, msg.ID)
			row, ok := decodeMessage(msg)
			if !ok {
				log.Warn("drop poison message", "id", msg.ID)
				continue
			}
			logs = append(logs, row)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if err := db.InsertLogs(ctx, logs); err != nil {
		return 0, err
	}
	if err := rdb.XAck(ctx, streamName, group, ids...).Err(); err != nil {
		return 0, err
	}
	return len(logs), nil
}

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
