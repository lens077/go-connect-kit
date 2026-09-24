package stale

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestGaugeFollowsState(t *testing.T) {
	if os.Getenv("STALE_GAUGE_TEST_CHILD") == "1" {
		testGaugeFollowsState(t)
		return
	}

	// OpenTelemetry's global provider and this package's metric registry intentionally live
	// for the process lifetime. Run the assertions in a fresh process so -count=N remains a
	// valid isolation check instead of replacing globals that production never replaces.
	command := exec.Command(os.Args[0], "-test.run=^TestGaugeFollowsState$", "-test.count=1")
	command.Env = append(os.Environ(), "STALE_GAUGE_TEST_CHILD=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func testGaugeFollowsState(t *testing.T) {
	reader := sdkmetric.NewManualReader()

	// Match application startup: resources register instruments during Fx construction,
	// then the OTel module installs the real provider in OnStart.
	var pgState, redisState State
	require.NoError(t, Register("pgpool", &pgState))
	require.NoError(t, Register("redisclient", &redisState))
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	assert.Equal(t, int64(0), gauge(t, reader, "pgpool"))
	assert.Equal(t, int64(0), gauge(t, reader, "redisclient"))

	pgState.Set(errors.New("rebuild failed"))
	assert.EqualError(t, pgState.Err(), "rebuild failed")
	assert.Equal(t, int64(1), gauge(t, reader, "pgpool"))
	assert.Equal(t, int64(0), gauge(t, reader, "redisclient"))

	pgState.Set(nil)
	redisState.Set(errors.New("rebuild failed"))
	assert.NoError(t, pgState.Err())
	assert.Equal(t, int64(0), gauge(t, reader, "pgpool"))
	assert.Equal(t, int64(1), gauge(t, reader, "redisclient"))
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
