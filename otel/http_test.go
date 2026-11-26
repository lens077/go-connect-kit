package otel

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiotel "go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInstrumentHTTPTracesRequestsButNotHealth(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := apiotel.GetTracerProvider()
	apiotel.SetTracerProvider(provider)
	t.Cleanup(func() { apiotel.SetTracerProvider(previous) })

	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := InstrumentHTTP(ok, "test-server")
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	for _, path := range []string{"/healthz", "/orders", "/healthz", "/users"} {
		resp, err := http.Get(server.URL + path)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusNoContent, resp.StatusCode, "path %s", path)
	}

	spans := recorder.Ended()
	require.Len(t, spans, 2, "only the two non-health requests should be traced")
	for _, span := range spans {
		// otelhttp v0.70 uses the HTTP method as the span name under the new
		// semconv; the operation argument no longer names the span. What this
		// test guards is the health filter and that instrumentation is active.
		assert.Equal(t, "GET", span.Name())
	}
}
