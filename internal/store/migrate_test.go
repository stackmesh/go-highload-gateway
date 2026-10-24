package store

import (
	"strings"
	"testing"
)

func TestMigrationEmbedded(t *testing.T) {
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS api_keys",
		"CREATE TABLE IF NOT EXISTS routes",
		"CREATE TABLE IF NOT EXISTS request_log",
		"key_hash BYTEA",
	} {
		if !strings.Contains(migrationSQL, want) {
			t.Fatalf("migration missing %q", want)
		}
	}
}
