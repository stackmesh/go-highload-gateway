// Package router matches a request path to a route snapshot.
//
// The table is immutable after publication. Routes are sorted by prefix
// length so the first match is the longest. A linear scan is the right
// structure here: route cardinality is a control-plane number (tens or
// hundreds), and the scan does not allocate.
package router

import (
	"sort"
	"strings"
	"time"
)

// Gate is the per-upstream circuit breaker. A nil gate is treated as closed.
type Gate interface {
	Allow() bool
	Success()
	Failure()
	Ignore()
}

type Route struct {
	Name     string
	Prefix   string
	Upstream string
	Timeout  time.Duration
	Breaker  Gate
}

type Table struct {
	routes []Route
}

func New(routes []Route) *Table {
	cp := make([]Route, 0, len(routes))
	for _, r := range routes {
		if r.Prefix == "" || r.Prefix[0] != '/' {
			continue
		}
		cp = append(cp, r)
	}
	sort.Slice(cp, func(i, j int) bool {
		if len(cp[i].Prefix) == len(cp[j].Prefix) {
			return cp[i].Prefix < cp[j].Prefix
		}
		return len(cp[i].Prefix) > len(cp[j].Prefix)
	})
	return &Table{routes: cp}
}

func (t *Table) Len() int {
	if t == nil {
		return 0
	}
	return len(t.routes)
}

// Match returns the longest prefix route for path.
// Prefix /v1/echo matches /v1/echo and /v1/echo/x, and does not match /v1/echo2.
func (t *Table) Match(path string) (Route, bool) {
	if t == nil {
		return Route{}, false
	}
	for i := range t.routes {
		if prefixMatch(path, t.routes[i].Prefix) {
			return t.routes[i], true
		}
	}
	return Route{}, false
}

func prefixMatch(path, prefix string) bool {
	if prefix == "/" {
		return path == "/" || strings.HasPrefix(path, "/")
	}
	if path == prefix {
		return true
	}
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if strings.HasSuffix(prefix, "/") {
		return true
	}
	return len(path) > len(prefix) && path[len(prefix)] == '/'
}
