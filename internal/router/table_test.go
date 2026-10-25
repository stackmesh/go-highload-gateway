package router

import "testing"

func TestLongestPrefix(t *testing.T) {
	table := New([]Route{
		{Name: "root", Prefix: "/v1"},
		{Name: "echo", Prefix: "/v1/echo"},
		{Name: "skip", Prefix: "relative"},
	})
	if table.Len() != 2 {
		t.Fatalf("len %d", table.Len())
	}
	got, ok := table.Match("/v1/echo/items")
	if !ok || got.Name != "echo" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
	got, ok = table.Match("/v1/echo")
	if !ok || got.Name != "echo" {
		t.Fatalf("exact: %+v", got)
	}
	got, ok = table.Match("/v1/echo2")
	if !ok || got.Name != "root" {
		t.Fatalf("boundary should fall through to /v1, got %+v ok=%v", got, ok)
	}
	if _, ok := table.Match("/healthz"); ok {
		t.Fatal("unexpected match")
	}
}

func TestSlashPrefix(t *testing.T) {
	table := New([]Route{{Name: "all", Prefix: "/"}})
	if _, ok := table.Match("/anywhere"); !ok {
		t.Fatal("expected slash prefix to match")
	}
}

func BenchmarkMatch(b *testing.B) {
	table := New([]Route{
		{Name: "a", Prefix: "/v1"},
		{Name: "b", Prefix: "/v1/echo"},
		{Name: "c", Prefix: "/v1/billing"},
		{Name: "d", Prefix: "/v1/users"},
		{Name: "e", Prefix: "/internal"},
		{Name: "f", Prefix: "/v1/echo/extra"},
		{Name: "g", Prefix: "/v2"},
		{Name: "h", Prefix: "/v1/search"},
	})
	b.ReportAllocs()
	var name string
	for i := 0; i < b.N; i++ {
		r, ok := table.Match("/v1/echo/items")
		if ok {
			name = r.Name
		}
	}
	if name != "b" {
		b.Fatalf("name %q", name)
	}
}
