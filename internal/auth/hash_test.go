package auth

import (
	"crypto/sha256"
	"testing"
)

func TestSumMatchesStdlib(t *testing.T) {
	const token = "gw_live_demo"
	got := Sum(token)
	want := sha256.Sum256([]byte(token))
	if got != want {
		t.Fatalf("got %x want %x", got, want)
	}
	if Sum("") != sha256.Sum256(nil) {
		t.Fatal("empty token")
	}
}

func BenchmarkSum(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Sum("gw_live_demo")
	}
}
