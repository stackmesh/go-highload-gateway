package proxy

import (
	"net"
	"net/http"
	"time"
)

// NewClient is the upstream pool. Timeouts for a single call come from the
// request context (the route timeout). The transport itself only bounds the
// dial so a dead upstream fails fast and does not pin a goroutine in DNS.
func NewClient() *http.Client {
	tr := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   500 * time.Millisecond,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		WriteBufferSize:     16 << 10,
		ReadBufferSize:      16 << 10,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   0,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
