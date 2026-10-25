package pool

import "testing"

func TestGetPut(t *testing.T) {
	b := Get()
	if b == nil || len(*b) != Size || cap(*b) < Size {
		t.Fatalf("buf len=%d cap=%d", len(*b), cap(*b))
	}
	(*b)[0] = 7
	Put(b)
	c := Get()
	if len(*c) != Size {
		t.Fatalf("reused len %d", len(*c))
	}
	Put(c)
}

func BenchmarkGetPut(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := Get()
		Put(buf)
	}
}
