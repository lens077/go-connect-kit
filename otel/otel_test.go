package otel

import (
	"context"
	"testing"
	"time"

	"github.com/lens077/go-connect-kit/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/zap"
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
	assert.NotPanics(t, func() {
		shutdown, _ := SetupOTelSDK(context.Background(), testAppInfo, options, zap.NewNop())
		if shutdown != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = shutdown(ctx)
		}
	})
}
