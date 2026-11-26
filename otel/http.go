package otel

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// healthPath is the only probe path used by our services: the readiness probe
// hits /healthz, while the liveness probe only dials the port. Tracing health
// checks pollutes RED metrics with synthetic traffic and keeps graphs busy
// during rollouts, so InstrumentHTTP filters this path out.
const healthPath = "/healthz"

// InstrumentHTTP wraps an HTTP handler with OpenTelemetry server
// instrumentation: a span per request plus the standard http.server.* metrics.
//
// The operation becomes the span name and the server_name metric attribute;
// callers should pass a stable per-service name such as "order-server" (see
// kit's template for the convention). Health probes on healthPath are excluded
// from both spans and metrics.
//
// Install it directly on http.Server.Handler when the server relies on
// http.Protocols{UnencryptedHTTP2} (Go 1.24+, the preferred setup: the
// deprecated x/net/http2/h2c wrapper is not needed for prior-knowledge h2c).
// If a caller still wraps with h2c.NewHandler — it is the only way to honour an
// HTTP/1.1 "Upgrade: h2c" request — keep this handler inside that wrapper so
// OTel sees the original HTTP request before the protocol upgrade.
func InstrumentHTTP(handler http.Handler, operation string) http.Handler {
	return otelhttp.NewHandler(handler, operation,
		otelhttp.WithFilter(func(req *http.Request) bool {
			return req.URL.Path != healthPath
		}),
	)
}
