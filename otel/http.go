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
// The returned handler must be installed inside h2c.NewHandler, so that OTel
// sees the original HTTP request before the protocol upgrade.
func InstrumentHTTP(handler http.Handler, operation string) http.Handler {
	return otelhttp.NewHandler(handler, operation,
		otelhttp.WithFilter(func(req *http.Request) bool {
			return req.URL.Path != healthPath
		}),
	)
}
