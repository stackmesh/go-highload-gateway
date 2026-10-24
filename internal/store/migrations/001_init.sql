CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key_hash BYTEA NOT NULL UNIQUE,
    tenant TEXT NOT NULL,
    rpm INTEGER NOT NULL CHECK (rpm > 0),
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS routes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    path_prefix TEXT NOT NULL,
    upstream TEXT NOT NULL,
    timeout_ms INTEGER NOT NULL DEFAULT 2000 CHECK (timeout_ms > 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);

-- Append-only audit table. The worker writes it with COPY, so there is no
-- secondary index on the hot insert path. Add a BRIN index on ts if you
-- start querying large ranges.
CREATE TABLE IF NOT EXISTS request_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    tenant TEXT NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status INTEGER NOT NULL,
    latency_us BIGINT NOT NULL,
    upstream TEXT NOT NULL
);
