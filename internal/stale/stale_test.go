package stale

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestGaugeFollowsState(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	var state State
	require.NoError(t, Register("pgpool", &state))
	assert.Equal(t, int64(0), gauge(t, reader, "pgpool"))

	state.Set(errors.New("rebuild failed"))
	assert.EqualError(t, state.Err(), "rebuild failed")
	assert.Equal(t, int64(1), gauge(t, reader, "pgpool"))

	state.Set(nil)
	assert.NoError(t, state.Err())
	assert.Equal(t, int64(0), gauge(t, reader, "pgpool"))
}

func gauge(t *testing.T, reader *sdkmetric.ManualReader, component string) int64 {
	t.Helper()
	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &metrics))
	for _, scope := range metrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != MetricName {
				continue
			}
			for _, point := range m.Data.(metricdata.Gauge[int64]).DataPoints {
				if value, ok := point.Attributes.Value("component"); ok && value.AsString() == component {
					return point.Value
				}
			}
		}
	}
	t.Fatalf("no %s data point for component %q", MetricName, component)
	return 0
}
