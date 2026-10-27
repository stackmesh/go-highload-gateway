package gateway

import (
	"net/http"
	"strconv"
)

var (
	bodyOK           = []byte(`{"status":"ok"}`)
	bodyNotReady     = []byte(`{"status":"not_ready"}`)
	bodyBadRequest   = []byte(`{"error":"bad_request"}`)
	bodyUnauthorized = []byte(`{"error":"unauthorized"}`)
	bodyNotFound     = []byte(`{"error":"route_not_found"}`)
	bodyRateLimited  = []byte(`{"error":"rate_limited"}`)
	bodyEntity       = []byte(`{"error":"body_too_large"}`)
	bodyBadGateway   = []byte(`{"error":"bad_gateway"}`)
	bodyUnavailable  = []byte(`{"error":"unavailable"}`)
	bodyTimeout      = []byte(`{"error":"upstream_timeout"}`)
	bodyInternal     = []byte(`{"error":"internal"}`)
	bodyMethod       = []byte(`{"error":"method_not_allowed"}`)
	bodyClientClosed = []byte(`{"error":"client_closed"}`)
)

func bodyFor(code int) []byte {
	switch code {
	case http.StatusBadRequest:
		return bodyBadRequest
	case http.StatusUnauthorized:
		return bodyUnauthorized
	case http.StatusNotFound:
		return bodyNotFound
	case http.StatusTooManyRequests:
		return bodyRateLimited
	case http.StatusRequestEntityTooLarge:
		return bodyEntity
	case http.StatusBadGateway:
		return bodyBadGateway
	case http.StatusServiceUnavailable:
		return bodyUnavailable
	case http.StatusGatewayTimeout:
		return bodyTimeout
	case http.StatusMethodNotAllowed:
		return bodyMethod
	case 499:
		return bodyClientClosed
	default:
		return bodyInternal
	}
}

func writeStatic(w http.ResponseWriter, code int) {
	writeJSON(w, code, bodyFor(code))
}

func writeJSON(w http.ResponseWriter, code int, body []byte) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)
	_, _ = w.Write(body)
}
