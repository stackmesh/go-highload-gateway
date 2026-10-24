package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("DATABASE_URL", "")
	cfg := Load()
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("http addr %q", cfg.HTTPAddr)
	}
	if cfg.BodyLimit != 1<<20 {
		t.Fatalf("body limit %d", cfg.BodyLimit)
	}
	if cfg.RefreshInterval != 5*time.Second {
		t.Fatalf("refresh %s", cfg.RefreshInterval)
	}
	if cfg.DemoAPIKey != "gw_live_demo" {
		t.Fatalf("demo key %q", cfg.DemoAPIKey)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":18080")
	t.Setenv("BODY_LIMIT", "4096")
	t.Setenv("REFRESH_INTERVAL", "2s")
	cfg := Load()
	if cfg.HTTPAddr != ":18080" || cfg.BodyLimit != 4096 || cfg.RefreshInterval != 2*time.Second {
		t.Fatalf("%+v", cfg)
	}
}
