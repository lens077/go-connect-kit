package otel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lens077/go-connect-kit/otelguard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiotel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// MetricOptions.Guard must actually wrap the OTLP exporter: an endpoint that
// rejects every export trips the circuit instead of printing an error per cycle.
func TestMetricGuardTripsOnForbiddenEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(server.Close)

	res, err := newResource(testAppInfo, "")
	require.NoError(t, err)

	guardConfig := otelguard.Config{MaxFailures: 1, InitialBackoff: time.Millisecond}
	provider, guard, err := newMeterProvider(context.Background(), res, testAppInfo, MetricOptions{
		Endpoint:       strings.TrimPrefix(server.URL, "http://"),
		ExportInterval: time.Minute,
		Guard:          &guardConfig,
	}, false, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, guard)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	require.NoError(t, guard.Export(context.Background(), &metricdata.ResourceMetrics{}))
	require.Equal(t, otelguard.StateOpen, guard.State())
	require.NotNil(t, guard.LastError())
	assert.Contains(t, guard.LastError().Error(), "403")
}

func TestMetricGuardDisabledKeepsRawExporter(t *testing.T) {
	res, err := newResource(testAppInfo, "")
	require.NoError(t, err)

	provider, guard, err := newMeterProvider(context.Background(), res, testAppInfo, MetricOptions{
		Endpoint: "127.0.0.1:4318",
		Guard:    &otelguard.Config{Disabled: true},
	}, false, zap.NewNop())
	require.NoError(t, err)
	assert.Nil(t, guard)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
}

// Guarding is the default: a service that only maps its endpoint gets backoff
// and circuit breaking without adding any code of its own.
func TestSetupGuardsMetricsByDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(server.Close)

	res, err := newResource(testAppInfo, "")
	require.NoError(t, err)

	provider, guard, err := newMeterProvider(context.Background(), res, testAppInfo, MetricOptions{
		Endpoint: strings.TrimPrefix(server.URL, "http://"),
	}, false, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, guard)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	assert.Equal(t, otelguard.DefaultMaxFailures, guard.Config().MaxFailures)
	assert.Equal(t, otelguard.DefaultProbeInterval, guard.Config().ProbeInterval)
}

// SetupOTelSDK installs the guard's silent error handler so SDK-internal
// failures stop reaching the application log while the circuit is open.
func TestSetupInstallsSilentErrorHandlerForGuardedMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(server.Close)

	shutdown, err := SetupOTelSDK(context.Background(), testAppInfo, Options{
		Metric: &MetricOptions{
			Endpoint: strings.TrimPrefix(server.URL, "http://"),
			Guard:    &otelguard.Config{MaxFailures: 1},
		},
	}, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	assert.IsType(t, &otelguard.SilentErrorHandler{}, apiotel.GetErrorHandler())
}
