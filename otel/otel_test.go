package otel

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lens077/go-connect-kit/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiotel "go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

var testAppInfo = meta.AppInfo{
	ID:          "test-service-id",
	Name:        "test-service",
	Host:        "127.0.0.1",
	Environment: "dev",
	Version:     "v1",
}

func TestModuleProvidesShutdown(t *testing.T) {
	var shutdown func(context.Context) error
	app := fx.New(
		fx.NopLogger,
		fx.Supply(testAppInfo, Options{}, zap.NewNop()),
		Module,
		fx.Populate(&shutdown),
	)
	require.NoError(t, app.Err())
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

func TestModuleInitializesWithoutOutputConsumer(t *testing.T) {
	core, observed := observer.New(zapcore.DebugLevel)
	app := fx.New(
		fx.NopLogger,
		fx.Supply(testAppInfo, Options{}, zap.New(core)),
		Module,
	)
	require.NoError(t, app.Err())
	assert.Zero(t, observed.Len(), "pipeline setup must wait for lifecycle start")

	require.NoError(t, app.Start(context.Background()))
	assert.Equal(t, 1, observed.FilterMessage("observability is disabled, skipping OpenTelemetry setup").Len())
	require.NoError(t, app.Stop(context.Background()))
}

func TestModuleDoesNotInitializeWhenFxNewFails(t *testing.T) {
	before := sdktrace.NewTracerProvider()
	apiotel.SetTracerProvider(before)

	sentinel := errors.New("later invoke failed")
	ratio := 1.0
	app := fx.New(
		fx.NopLogger,
		fx.Supply(testAppInfo, Options{
			Trace: &TraceOptions{Endpoint: "127.0.0.1:1", SampleRatio: &ratio},
		}, zap.NewNop()),
		Module,
		fx.Invoke(func() error { return sentinel }),
	)
	require.ErrorIs(t, app.Err(), sentinel)
	assert.Equal(t, before, apiotel.GetTracerProvider(), "Fx construction failure must not start SDK workers or replace globals")
}

func TestModuleFlushesEnabledTracePipelineOnStop(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/traces" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = io.Copy(io.Discard, request.Body)
		requests.Add(1)
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Cleanup(func() { apiotel.SetTracerProvider(sdktrace.NewTracerProvider()) })

	ratio := 1.0
	app := fx.New(
		fx.NopLogger,
		fx.Supply(testAppInfo, Options{
			Trace: &TraceOptions{
				Endpoint:    strings.TrimPrefix(server.URL, "http://"),
				SampleRatio: &ratio,
			},
		}, zap.NewNop()),
		Module,
	)
	require.NoError(t, app.Err())
	require.NoError(t, app.Start(context.Background()))

	_, span := apiotel.Tracer("module-lifecycle-test").Start(context.Background(), "flush-on-stop")
	span.End()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Stop(stopCtx))
	assert.GreaterOrEqual(t, requests.Load(), int32(1))
}

func TestNewResource(t *testing.T) {
	resource, err := newResource(testAppInfo, "config-center")
	require.NoError(t, err)
	assert.NotEmpty(t, resource.SchemaURL(), "empty schema URL indicates a semconv version mismatch")

	attributes := map[string]string{}
	for _, attribute := range resource.Attributes() {
		attributes[string(attribute.Key)] = attribute.Value.Emit()
	}
	assert.Equal(t, "test-service", attributes["service.name"])
	assert.Equal(t, "v1", attributes["service.version"])
	assert.Equal(t, meta.Version, attributes["app.build_id"])
	assert.Equal(t, "test-service-id", attributes["service.instance.id"])
	assert.Equal(t, "dev", attributes["deployment.environment.name"])
	assert.Equal(t, "config-center", attributes["service.namespace"])
	assert.Equal(t, "go", attributes["process.runtime.name"])
	assert.NotEmpty(t, attributes["process.runtime.version"])
	assert.NotEmpty(t, attributes["telemetry.sdk.version"])
}

func TestNewPropagator(t *testing.T) {
	propagator := newPropagator()
	assert.Contains(t, propagator.Fields(), "traceparent")
	assert.Contains(t, propagator.Fields(), "baggage")
}

func TestSampleRatio(t *testing.T) {
	value := func(ratio float64) *float64 { return &ratio }
	tests := []struct {
		name    string
		options TraceOptions
		want    float64
	}{
		{name: "unset", want: 1},
		{name: "explicit zero", options: TraceOptions{SampleRatio: value(0)}, want: 0},
		{name: "negative", options: TraceOptions{SampleRatio: value(-0.5)}, want: 0},
		{name: "fraction", options: TraceOptions{SampleRatio: value(0.25)}, want: 0.25},
		{name: "one", options: TraceOptions{SampleRatio: value(1)}, want: 1},
		{name: "above one", options: TraceOptions{SampleRatio: value(7)}, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, sampleRatio(test.options, zap.NewNop()))
		})
	}
}

func TestTLSClientConfig(t *testing.T) {
	assert.Nil(t, tlsClientConfig(TLSOptions{}, zap.NewNop()))

	insecure := tlsClientConfig(TLSOptions{Enabled: true, InsecureSkipVerify: true}, zap.NewNop())
	require.NotNil(t, insecure)
	assert.True(t, insecure.InsecureSkipVerify)
	assert.Nil(t, insecure.RootCAs)

	badCA := tlsClientConfig(TLSOptions{Enabled: true, CAPEM: "not a pem"}, zap.NewNop())
	require.NotNil(t, badCA)
	assert.Nil(t, badCA.RootCAs)
}

func TestSQLSpanName(t *testing.T) {
	assert.Equal(t, "GetCartItems", SQLSpanName("-- name: GetCartItems :many\nSELECT 1"))
	assert.Equal(t, "InsertCartItem", SQLSpanName("-- name: InsertCartItem :one\nINSERT INTO x VALUES (1)"))
	assert.Equal(t, "SELECT", SQLSpanName("\n-- comment\n SELECT id FROM t"))
	assert.Equal(t, "BEGIN", SQLSpanName("BEGIN"))
	assert.Equal(t, "unknown", SQLSpanName("-- comment only\n"))

	longStatement := "-- name: Bounded :many\nSELECT " + string(make([]byte, 4096))
	assert.Equal(t, "Bounded", SQLSpanName(longStatement))
}

func TestSetupOTelSDKDisabledIsNoop(t *testing.T) {
	shutdown, err := SetupOTelSDK(context.Background(), testAppInfo, Options{}, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

func TestSetupOTelSDKMinimalConfigDoesNotPanic(t *testing.T) {
	ratio := 0.5
	options := Options{
		Trace: &TraceOptions{
			Endpoint:    "localhost:4318",
			SampleRatio: &ratio,
		},
		Metric: &MetricOptions{
			Endpoint:       "localhost:4318",
			ExportInterval: 30 * time.Second,
		},
		Logging: &LoggingOptions{Endpoint: "localhost:4318"},
	}
	shutdown, err := SetupOTelSDK(context.Background(), testAppInfo, options, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = shutdown(ctx)
}
