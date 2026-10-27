package gateway

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	gatewayv1 "github.com/stackmesh/go-highload-gateway/api/gen/gateway/v1"
	"github.com/stackmesh/go-highload-gateway/internal/pool"
)

// GRPC adapts the data plane to the protobuf ingress.
type GRPC struct {
	gatewayv1.UnimplementedGatewayServer
	core *Core
}

func NewGRPC(core *Core) *GRPC {
	return &GRPC{core: core}
}

func (g *GRPC) Proxy(ctx context.Context, req *gatewayv1.ProxyRequest) (*gatewayv1.ProxyResponse, error) {
	start := time.Now()
	c := g.core
	path, rawQuery := splitPath(req.GetPath())
	method := req.GetMethod()
	if method == "" {
		method = http.MethodGet
	}
	if !validMethod(method) || !validPath(path) {
		c.observe(admission{}, http.StatusBadRequest, start)
		return &gatewayv1.ProxyResponse{Status: http.StatusBadRequest, Body: bodyBadRequest}, nil
	}
	body := req.GetBody()
	if int64(len(body)) > c.bodyLimit {
		c.observe(admission{}, http.StatusRequestEntityTooLarge, start)
		return &gatewayv1.ProxyResponse{Status: http.StatusRequestEntityTooLarge, Body: bodyEntity}, nil
	}

	a, code := c.admit(path, req.GetApiKey())
	if code != 0 {
		c.observe(a, code, start)
		return &gatewayv1.ProxyResponse{Status: int32(code), Body: bodyFor(code)}, nil
	}
	a, code = c.arm(a)
	if code != 0 {
		c.observe(a, code, start)
		return &gatewayv1.ProxyResponse{Status: int32(code), Body: bodyFor(code)}, nil
	}
	result := resultFail
	defer func() { a.done(result) }()

	hdr := make(http.Header, len(req.GetHeaders()))
	for k, v := range req.GetHeaders() {
		hdr.Set(k, v)
	}
	resp, err := c.forward(ctx, a, method, path, rawQuery, hdr, bytes.NewReader(body), int64(len(body)), "")
	if err != nil {
		status, kind := classifyForwardErr(err)
		result = kind
		c.observe(a, status, start)
		c.emit(a, method, path, status, start)
		return &gatewayv1.ProxyResponse{Status: int32(status), Body: bodyFor(status), LatencyUs: time.Since(start).Microseconds()}, nil
	}
	defer resp.Body.Close()

	buf := pool.Get()
	defer pool.Put(buf)
	payload, err := readLimited(resp.Body, c.bodyLimit, *buf)
	if err != nil {
		result = resultFail
		c.observe(a, http.StatusBadGateway, start)
		c.emit(a, method, path, http.StatusBadGateway, start)
		return &gatewayv1.ProxyResponse{Status: http.StatusBadGateway, Body: bodyBadGateway, LatencyUs: time.Since(start).Microseconds()}, nil
	}
	if resp.StatusCode >= 500 {
		result = resultFail
	} else {
		result = resultOK
	}
	out := &gatewayv1.ProxyResponse{
		Status:    int32(resp.StatusCode),
		Headers:   headerMap(resp.Header),
		Body:      payload,
		LatencyUs: time.Since(start).Microseconds(),
	}
	c.observe(a, resp.StatusCode, start)
	c.emit(a, method, path, resp.StatusCode, start)
	return out, nil
}

func readLimited(r io.Reader, limit int64, scratch []byte) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.CopyBuffer(&buf, io.LimitReader(r, limit+1), scratch); err != nil {
		return nil, err
	}
	if int64(buf.Len()) > limit {
		return nil, io.ErrUnexpectedEOF
	}
	return buf.Bytes(), nil
}

func headerMap(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	n := 0
	for k, vs := range h {
		if hop(k) || len(vs) == 0 || n == 32 {
			continue
		}
		out[k] = vs[0]
		n++
	}
	return out
}
