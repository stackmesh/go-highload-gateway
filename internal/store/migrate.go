package store

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/001_init.sql
var migrationSQL string

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(714217)`); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	var ready bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'schema_migrations'
		)`).Scan(&ready); err != nil {
		return err
	}
	if ready {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version = '001'`).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return tx.Commit(ctx)
		}
	}
	if _, err := tx.Exec(ctx, migrationSQL); err != nil {
		return fmt.Errorf("apply 001: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ('001') ON CONFLICT DO NOTHING`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Seed inserts the demo and limited keys once, and upserts the echo route
// so UPSTREAM_URL follows the environment across restarts.
func (s *Store) Seed(ctx context.Context, demoHash, limitedHash [32]byte, upstream string, timeoutMs int) error {
	if timeoutMs <= 0 {
		timeoutMs = 2000
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO api_keys (key_hash, tenant, rpm)
		VALUES ($1, $2, $3)
		ON CONFLICT (key_hash) DO NOTHING`, demoHash[:], demoTenant, DemoRPM); err != nil {
		return fmt.Errorf("seed demo key: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO api_keys (key_hash, tenant, rpm)
		VALUES ($1, $2, $3)
		ON CONFLICT (key_hash) DO NOTHING`, limitedHash[:], limitTenant, LimitedRPM); err != nil {
		return fmt.Errorf("seed limited key: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO routes (name, path_prefix, upstream, timeout_ms)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO UPDATE
		SET upstream = EXCLUDED.upstream,
		    timeout_ms = EXCLUDED.timeout_ms,
		    enabled = TRUE`, echoRoute, echoPrefix, upstream, timeoutMs); err != nil {
		return fmt.Errorf("seed route: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *Store) Load(ctx context.Context) ([]APIKey, []Route, error) {
	krows, err := s.pool.Query(ctx, `SELECT id, key_hash, tenant, rpm FROM api_keys WHERE active`)
	if err != nil {
		return nil, nil, err
	}
	defer krows.Close()
	var keys []APIKey
	for krows.Next() {
		var (
			id     int64
			raw    []byte
			tenant string
			rpm    int
		)
		if err := krows.Scan(&id, &raw, &tenant, &rpm); err != nil {
			return nil, nil, err
		}
		sum, ok := as32(raw)
		if !ok || id < 0 {
			continue
		}
		keys = append(keys, APIKey{ID: uint64(id), Hash: sum, Tenant: tenant, RPM: rpm})
	}
	if err := krows.Err(); err != nil {
		return nil, nil, err
	}

	rrows, err := s.pool.Query(ctx, `SELECT name, path_prefix, upstream, timeout_ms FROM routes WHERE enabled`)
	if err != nil {
		return nil, nil, err
	}
	defer rrows.Close()
	var routes []Route
	for rrows.Next() {
		var (
			name, prefix, upstream string
			timeoutMs              int
		)
		if err := rrows.Scan(&name, &prefix, &upstream, &timeoutMs); err != nil {
			return nil, nil, err
		}
		routes = append(routes, Route{
			Name:     name,
			Prefix:   prefix,
			Upstream: upstream,
			Timeout:  ms(timeoutMs),
		})
	}
	if err := rrows.Err(); err != nil {
		return nil, nil, err
	}
	return keys, routes, nil
}

func (s *Store) InsertLogs(ctx context.Context, logs []Log) error {
	if len(logs) == 0 {
		return nil
	}
	rows := make([][]any, len(logs))
	for i, l := range logs {
		rows[i] = []any{l.At, l.Tenant, l.Method, l.Path, l.Status, l.LatencyUs, l.Upstream}
	}
	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"request_log"},
		[]string{"ts", "tenant", "method", "path", "status", "latency_us", "upstream"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy request_log: %w", err)
	}
	return nil
}

func as32(b []byte) ([32]byte, bool) {
	var out [32]byte
	if len(b) != 32 {
		return out, false
	}
	copy(out[:], b)
	return out, true
}
