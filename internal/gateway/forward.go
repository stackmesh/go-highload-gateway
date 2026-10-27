package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// forward calls the upstream and leaves resp.Body responsible for canceling
// the route timeout. Closing the body is required.
func (c *Core) forward(ctx context.Context, a admission, method, path, rawQuery string, hdr http.Header, body io.Reader, contentLength int64, remote string) (*http.Response, error) {
	timeout := a.route.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)

	target, err := joinURL(a.route.Upstream, path, rawQuery)
	if err != nil {
		cancel()
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		cancel()
		return nil, err
	}
	req.ContentLength = contentLength
	copyInbound(req.Header, hdr)
	if remote != "" {
		req.Header.Set("X-Forwarded-For", forwardedFor(hdr, remote))
	}
	req.Header.Set("X-Tenant", a.key.Tenant)
	req.Header.Set("X-Route", a.route.Name)

	resp, err := c.client.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

func joinURL(base, path, rawQuery string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("upstream %q is not an absolute URL", base)
	}
	u.Path = path
	u.RawPath = ""
	u.RawQuery = rawQuery
	u.Fragment = ""
	return u.String(), nil
}

func copyInbound(dst, src http.Header) {
	for k, vs := range src {
		if hop(k) || k == "Authorization" || k == "X-Api-Key" {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func copyOutbound(dst, src http.Header) {
	for k, vs := range src {
		if hop(k) {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func hop(k string) bool {
	switch k {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	default:
		return false
	}
}

func forwardedFor(h http.Header, remote string) string {
	ip := remote
	if host, _, err := net.SplitHostPort(remote); err == nil {
		ip = host
	}
	if prior := h.Get("X-Forwarded-For"); prior != "" {
		return prior + ", " + ip
	}
	return ip
}

func timeoutErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func splitPath(path string) (string, string) {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		return path[:i], path[i+1:]
	}
	return path, ""
}

func validMethod(m string) bool {
	if m == "" || len(m) > 16 {
		return false
	}
	for i := 0; i < len(m); i++ {
		if m[i] < 'A' || m[i] > 'Z' {
			return false
		}
	}
	return true
}

func validPath(p string) bool {
	if len(p) == 0 || len(p) > 2048 || p[0] != '/' {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] < 0x20 || p[i] == 0x7f {
			return false
		}
	}
	return true
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.Clone(s[:n])
}
