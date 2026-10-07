package otelguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errExport = errors.New("failed to send metrics to https://otlp-dev.apikv.com:443: 403 Forbidden (body: (empty))")

type fakeExporter struct {
	mu          sync.Mutex
	calls       int
	flushes     int
	shutdowns   int
	err         error
	flushErr    error
	shutdownErr error
}

func (fake *fakeExporter) Temporality(metric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (fake *fakeExporter) Aggregation(metric.InstrumentKind) metric.Aggregation {
	return metric.AggregationDefault{}
}

func (fake *fakeExporter) Export(context.Context, *metricdata.ResourceMetrics) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	return fake.err
}

func (fake *fakeExporter) ForceFlush(context.Context) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.flushes++
	return fake.flushErr
}

func (fake *fakeExporter) Shutdown(context.Context) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.shutdowns++
	return fake.shutdownErr
}

func (fake *fakeExporter) setError(err error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.err = err
}

func (fake *fakeExporter) callCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls
}

func (fake *fakeExporter) flushCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.flushes
}

func (fake *fakeExporter) shutdownCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.shutdowns
}

// testClock makes backoff windows deterministic: no test sleeps for a backoff.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
}

func newTestGuard(t *testing.T, fake *fakeExporter, cfg Config) (*GuardedExporter, *testClock, *observer.ObservedLogs) {
	t.Helper()
	clock := newTestClock()
	core, logs := observer.New(zapcore.DebugLevel)
	cfg.Jitter = 0
	guard := New(fake, cfg, WithLogger(zap.New(core)))
	guard.now = clock.Now
	return guard, clock, logs
}

// ntfyRecorder captures what the guard publishes.
type ntfyRecorder struct {
	server   *httptest.Server
	requests chan *http.Request
	bodies   chan string
}

func newNtfyRecorder(t *testing.T) *ntfyRecorder {
	t.Helper()
	recorder := &ntfyRecorder{
		requests: make(chan *http.Request, 16),
		bodies:   make(chan string, 16),
	}
	recorder.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := readAll(request)
		recorder.requests <- request
		recorder.bodies <- body
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(recorder.server.Close)
	return recorder
}

func readAll(request *http.Request) (string, error) {
	if request.Body == nil {
		return "", nil
	}
	defer request.Body.Close()
	body, err := io.ReadAll(request.Body)
	return string(body), err
}

func statusError(code int, message string) error {
	return status.Error(codes.Code(code), message)
}

func (recorder *ntfyRecorder) next(t *testing.T) (*http.Request, string) {
	t.Helper()
	select {
	case request := <-recorder.requests:
		body := <-recorder.bodies
		return request, body
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an ntfy notification")
		return nil, ""
	}
}

func (recorder *ntfyRecorder) assertSilent(t *testing.T) {
	t.Helper()
	select {
	case request := <-recorder.requests:
		t.Fatalf("unexpected ntfy notification: %s", request.URL)
	case <-time.After(200 * time.Millisecond):
	}
}

func (recorder *ntfyRecorder) ntfyConfig() NtfyConfig {
	return NtfyConfig{URL: recorder.server.URL, Topic: "otel-guard-test", Token: "s3cret"}
}

func export(t *testing.T, guard *GuardedExporter) {
	t.Helper()
	require.NoError(t, guard.Export(context.Background(), &metricdata.ResourceMetrics{}))
}

// F1 + F3: failures below the threshold pass through, are counted, and produce
// neither per-failure error logs nor a non-nil Export result.
func TestFailuresBelowThresholdStayQuiet(t *testing.T) {
	fake := &fakeExporter{err: errExport}
	guard, clock, logs := newTestGuard(t, fake, Config{MaxFailures: 6, InitialBackoff: time.Millisecond})

	for range 5 {
		export(t, guard)
		clock.Advance(10 * time.Millisecond)
	}

	assert.Equal(t, StateClosed, guard.State())
	assert.False(t, guard.IsOpen())
	assert.Equal(t, 5, fake.callCount())
	assert.Equal(t, 0, logs.FilterLevelExact(zapcore.ErrorLevel).Len())
	assert.Equal(t, 0, logs.FilterLevelExact(zapcore.WarnLevel).Len())
}

// F3 + F5: the threshold opens the circuit, alerts exactly once, and every
// later export returns nil without touching the inner exporter.
func TestCircuitOpensAlertsOnceAndStopsExporting(t *testing.T) {
	recorder := newNtfyRecorder(t)
	fake := &fakeExporter{err: errExport}
	guard, clock, logs := newTestGuard(t, fake, Config{
		MaxFailures:    2,
		InitialBackoff: time.Millisecond,
		Ntfy:           recorder.ntfyConfig(),
	})

	export(t, guard)
	clock.Advance(time.Second)
	export(t, guard)

	require.Equal(t, StateOpen, guard.State())
	require.True(t, guard.IsOpen())
	require.Equal(t, 2, fake.callCount())
	require.Equal(t, 1, logs.FilterLevelExact(zapcore.ErrorLevel).Len())

	request, body := recorder.next(t)
	assert.Equal(t, "Bearer s3cret", request.Header.Get("Authorization"))
	assert.Equal(t, "urgent", request.Header.Get("Priority"))
	assert.Contains(t, request.Header.Get("Title"), "指标导出熔断")
	assert.Contains(t, body, "403")
	assert.Contains(t, body, "permanent")

	for range 3 {
		export(t, guard)
		clock.Advance(time.Minute)
	}
	assert.Equal(t, 2, fake.callCount(), "open circuit must not call the inner exporter")
	recorder.assertSilent(t)
}

// F2: a failure inside the backoff window drops the export instead of retrying.
func TestExponentialBackoffSkipsExports(t *testing.T) {
	fake := &fakeExporter{err: errExport}
	guard, clock, _ := newTestGuard(t, fake, Config{MaxFailures: 3, InitialBackoff: 2 * time.Second})

	export(t, guard)
	require.Equal(t, 1, fake.callCount())

	export(t, guard)
	assert.Equal(t, 1, fake.callCount(), "export inside the backoff window must be dropped")

	clock.Advance(1999 * time.Millisecond)
	export(t, guard)
	assert.Equal(t, 1, fake.callCount())

	clock.Advance(2 * time.Millisecond)
	export(t, guard)
	assert.Equal(t, 2, fake.callCount())

	assert.Equal(t, 2*time.Second, guard.backoff(1))
	assert.Equal(t, 4*time.Second, guard.backoff(2))
	assert.Equal(t, DefaultMaxBackoff, guard.backoff(20))
}

// F6: after ProbeInterval one export is allowed; success closes the circuit and
// sends the recovery notice.
func TestHalfOpenProbeRecoversAndNotifies(t *testing.T) {
	recorder := newNtfyRecorder(t)
	fake := &fakeExporter{err: errExport}
	guard, clock, logs := newTestGuard(t, fake, Config{
		MaxFailures:   1,
		ProbeInterval: 5 * time.Minute,
		Ntfy:          recorder.ntfyConfig(),
	})

	export(t, guard)
	require.Equal(t, StateOpen, guard.State())
	recorder.next(t)

	clock.Advance(4 * time.Minute)
	export(t, guard)
	assert.Equal(t, 1, fake.callCount(), "must not probe before ProbeInterval")

	clock.Advance(time.Minute)
	fake.setError(nil)
	export(t, guard)

	assert.Equal(t, StateClosed, guard.State())
	assert.False(t, guard.IsOpen())
	assert.Equal(t, 2, fake.callCount())
	assert.Nil(t, guard.LastError())
	assert.Equal(t, 1, logs.FilterMessage("otelguard: metric export recovered").Len())

	request, body := recorder.next(t)
	assert.Equal(t, "default", request.Header.Get("Priority"))
	assert.Contains(t, request.Header.Get("Title"), "指标导出恢复")
	assert.Contains(t, body, "已恢复")
}

// F6: a failed probe re-opens the circuit, resets the probe window, and does
// not repeat the alert.
func TestFailedProbeReopensSilently(t *testing.T) {
	recorder := newNtfyRecorder(t)
	fake := &fakeExporter{err: errExport}
	guard, clock, logs := newTestGuard(t, fake, Config{
		MaxFailures:   1,
		ProbeInterval: 5 * time.Minute,
		Ntfy:          recorder.ntfyConfig(),
	})

	export(t, guard)
	recorder.next(t)
	require.Equal(t, 1, logs.FilterLevelExact(zapcore.ErrorLevel).Len())

	clock.Advance(5 * time.Minute)
	export(t, guard)
	assert.Equal(t, StateOpen, guard.State())
	assert.Equal(t, 2, fake.callCount())

	clock.Advance(4 * time.Minute)
	export(t, guard)
	assert.Equal(t, 2, fake.callCount(), "probe window must restart after a failed probe")

	clock.Advance(time.Minute)
	export(t, guard)
	assert.Equal(t, 3, fake.callCount())

	assert.Equal(t, 1, logs.FilterLevelExact(zapcore.ErrorLevel).Len(), "one outage, one alert")
	recorder.assertSilent(t)
}

// F4: the global handler is silent while the circuit is open and rate-limits
// itself otherwise.
func TestSilentErrorHandlerSuppressesWhileOpen(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	fake := &fakeExporter{err: errExport}
	guard, clock, _ := newTestGuard(t, fake, Config{MaxFailures: 1})

	handler := NewSilentErrorHandler(guard.IsOpen, zap.New(core), time.Minute)
	handler.Handle(errors.New("periodic reader export failed"))
	handler.Handle(errors.New("periodic reader export failed"))
	assert.Equal(t, 1, logs.Len(), "closed state logs at most one warning per interval")

	export(t, guard)
	require.True(t, guard.IsOpen())
	require.NoError(t, guard.Export(context.Background(), &metricdata.ResourceMetrics{}))

	handler.Handle(errors.New("periodic reader export failed"))
	assert.Equal(t, 1, logs.Len(), "open state must stay silent")

	// A new outage after recovery may log again.
	fake.setError(nil)
	clock.Advance(5 * time.Minute)
	export(t, guard)
	require.False(t, guard.IsOpen())
	handler = NewSilentErrorHandler(guard.IsOpen, zap.New(core), time.Minute)
	handler.Handle(errors.New("periodic reader export failed"))
	assert.Equal(t, 2, logs.Len())
}

// F7: HardShutdown closes the inner exporter and never probes again.
func TestHardShutdownClosesInnerExporter(t *testing.T) {
	fake := &fakeExporter{err: errExport}
	guard, clock, _ := newTestGuard(t, fake, Config{
		MaxFailures:     1,
		HardShutdown:    true,
		ShutdownTimeout: time.Second,
	})

	export(t, guard)
	require.Equal(t, StateOpen, guard.State())

	require.Eventually(t, func() bool { return fake.shutdownCount() == 1 }, 2*time.Second, 10*time.Millisecond)

	clock.Advance(time.Hour)
	for range 3 {
		export(t, guard)
	}
	assert.Equal(t, 1, fake.callCount(), "hard shutdown must never probe")
	assert.Equal(t, 1, fake.shutdownCount())
}

// F8: ForceFlush short-circuits while open and delegates while closed;
// Temporality and Aggregation always delegate.
func TestForceFlushAndDelegation(t *testing.T) {
	fake := &fakeExporter{err: errExport}
	guard, _, _ := newTestGuard(t, fake, Config{MaxFailures: 1})

	require.NoError(t, guard.ForceFlush(context.Background()))
	assert.Equal(t, 1, fake.flushCount())

	export(t, guard)
	require.Equal(t, StateOpen, guard.State())
	require.NoError(t, guard.ForceFlush(context.Background()))
	assert.Equal(t, 1, fake.flushCount(), "open circuit must not flush the inner exporter")

	assert.Equal(t, metricdata.CumulativeTemporality, guard.Temporality(metric.InstrumentKindCounter))
	assert.IsType(t, metric.AggregationDefault{}, guard.Aggregation(metric.InstrumentKindCounter))
}

// F8: Shutdown delegates once, marks the guard open, and stops all exports.
func TestShutdownDelegatesOnceAndStopsExports(t *testing.T) {
	fake := &fakeExporter{}
	guard, _, _ := newTestGuard(t, fake, Config{MaxFailures: 1})

	export(t, guard)
	require.Equal(t, 1, fake.callCount())

	require.NoError(t, guard.Shutdown(context.Background()))
	require.NoError(t, guard.Shutdown(context.Background()))
	assert.Equal(t, 1, fake.shutdownCount())
	assert.Equal(t, StateOpen, guard.State())
	assert.True(t, guard.IsOpen())

	export(t, guard)
	assert.Equal(t, 1, fake.callCount(), "shutdown guard must not export")
	require.NoError(t, guard.ForceFlush(context.Background()))
	assert.Equal(t, 0, fake.flushCount())
}

// A disabled guard is a pass-through: the inner error stays visible and no
// state is tracked.
func TestDisabledGuardPassesErrorsThrough(t *testing.T) {
	fake := &fakeExporter{err: errExport}
	guard, _, _ := newTestGuard(t, fake, Config{Disabled: true})

	require.ErrorIs(t, guard.Export(context.Background(), &metricdata.ResourceMetrics{}), errExport)
	assert.False(t, guard.IsOpen())
	assert.Equal(t, StateClosed, guard.State())
}

// A disabled guard is only a pass-through: it must not swallow the inner
// exporter's lifecycle calls.
func TestDisabledGuardStillDelegatesLifecycle(t *testing.T) {
	fake := &fakeExporter{}
	guard, _, _ := newTestGuard(t, fake, Config{Disabled: true})

	require.NoError(t, guard.ForceFlush(context.Background()))
	require.NoError(t, guard.Shutdown(context.Background()))
	assert.Equal(t, 1, fake.flushCount())
	assert.Equal(t, 1, fake.shutdownCount())
}

func TestClassifySeparatesPermanentFromTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want ErrorClass
	}{
		{"nil", nil, ClassUnknown},
		{"http 400", errors.New("failed to send metrics to https://x/v1/metrics: 400 Bad Request (body: bad)"), ClassPermanent},
		{"http 401", errors.New("failed to send metrics to https://x/v1/metrics: 401 Unauthorized (body: (empty))"), ClassPermanent},
		{"http 403", errExport, ClassPermanent},
		{"http 404", errors.New("failed to send metrics to https://x/v1/metrics: 404 Not Found (body: (empty))"), ClassPermanent},
		{"http 408", errors.New("failed to send metrics to https://x/v1/metrics: 408 Request Timeout (body: (empty))"), ClassTransient},
		{"http 500", errors.New("failed to send metrics to https://x/v1/metrics: 500 Internal Server Error (body: (empty))"), ClassTransient},
		{"http 503", errors.New("failed to send metrics to https://x/v1/metrics: 503 Service Unavailable (body: (empty))"), ClassTransient},
		{"exporter retry exhausted", errors.New("retry-able request failure: body: (empty)"), ClassTransient},
		{"deadline", fmt.Errorf("upload: %w", context.DeadlineExceeded), ClassTransient},
		{"connection refused", errors.New("Post \"https://x/v1/metrics\": dial tcp 127.0.0.1:4318: connect: connection refused"), ClassTransient},
		{"unknown", errors.New("something else"), ClassUnknown},
		{"grpc unavailable", statusError(14, "connection refused"), ClassTransient},
		{"grpc unauthenticated", statusError(16, "missing token"), ClassPermanent},
		{"grpc permission denied", statusError(7, "nope"), ClassPermanent},
		{"grpc invalid argument", statusError(3, "bad body"), ClassPermanent},
		{"grpc not found", statusError(5, "no route"), ClassPermanent},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.want, Classify(testCase.err))
		})
	}
}

func TestNilInnerExporterIsInert(t *testing.T) {
	guard := New(nil, DefaultConfig())
	assert.NoError(t, guard.Export(context.Background(), &metricdata.ResourceMetrics{}))
	assert.Equal(t, StateClosed, guard.State())
	assert.NoError(t, guard.ForceFlush(context.Background()))
	assert.NoError(t, guard.Shutdown(context.Background()))
}

func TestConfigFromEnvOverridesDefaults(t *testing.T) {
	t.Setenv("OTEL_GUARD_SERVICE", "order")
	t.Setenv("OTEL_GUARD_MAX_FAILURES", "3")
	t.Setenv("OTEL_GUARD_INITIAL_BACKOFF", "500ms")
	t.Setenv("OTEL_GUARD_HARD_SHUTDOWN", "true")
	t.Setenv("OTEL_GUARD_NTFY_URL", "https://ntfy.example.com")
	t.Setenv("OTEL_GUARD_NTFY_TOPIC", "otel-guard")
	t.Setenv("OTEL_GUARD_NTFY_TOKEN", "token")

	cfg := ConfigFromEnv()
	assert.Equal(t, "order", cfg.Service)
	assert.Equal(t, 3, cfg.MaxFailures)
	assert.Equal(t, 500*time.Millisecond, cfg.InitialBackoff)
	assert.True(t, cfg.HardShutdown)
	assert.Equal(t, "https://ntfy.example.com", cfg.Ntfy.URL)
	assert.Equal(t, "otel-guard", cfg.Ntfy.Topic)
	assert.Equal(t, "token", cfg.Ntfy.Token)
	assert.Equal(t, DefaultMaxBackoff, cfg.MaxBackoff, "unset variables keep the defaults")
}
