// Package catalog is the immutable data-plane snapshot.
// The gateway publishes a new pointer after each control-plane refresh.
// Request goroutines load it atomically and never lock.
package catalog

import "github.com/stackmesh/go-highload-gateway/internal/router"

type Key struct {
	ID     uint64
	Tenant string
	RPM    int
}

type Snapshot struct {
	Keys   map[[32]byte]Key
	Routes *router.Table
}
