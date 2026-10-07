// Package otelguard wraps an OpenTelemetry metric exporter with error
// classification, exponential backoff, a circuit breaker and ntfy alerts.
//
// A microservice whose OTLP endpoint is misconfigured (403), unavailable (5xx)
// or unreachable currently prints an export error on every collection cycle.
// The guard turns that into: retry with backoff, circuit-break after
// MaxFailures, one alert, no more error lines while open, and automatic
// recovery through a half-open probe.
//
// Every Export call returns nil, so the SDK's periodic reader never forwards an
// export failure to the global error handler. Pair the guard with
// NewSilentErrorHandler to silence the rest of the SDK as well.
package otelguard

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorClass separates failures that retrying can fix from failures only a
// human can fix. It drives the alert text, not the threshold: a config error
// still needs the same number of observations before the circuit opens.
type ErrorClass int

const (
	// ClassUnknown covers errors the classifier does not recognize; treated as
	// transient.
	ClassUnknown ErrorClass = iota
	// ClassTransient covers 5xx/408/429, timeouts and network failures.
	ClassTransient
	// ClassPermanent covers 400/401/403/404 (and their gRPC equivalents).
	ClassPermanent
)

func (class ErrorClass) String() string {
	switch class {
	case ClassPermanent:
		return "permanent"
	case ClassTransient:
		return "transient"
	default:
		return "unknown"
	}
}

var httpStatusPattern = regexp.MustCompile(`\b([1-5]\d{2})\b\s+[A-Z]`)

// Classify inspects an exporter error. OTLP/HTTP errors carry the status only
// in their message (the exporter types are unexported), so the classifier also
// parses the status line; gRPC exporters expose a status code and are read
// through status.FromError. Anything unrecognized is transient.
func Classify(err error) ErrorClass {
	if err == nil {
		return ClassUnknown
	}

	// Cancelled and deadline-bound work is always worth retrying.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ClassTransient
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return ClassTransient
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return ClassTransient
	}

	if grpcStatus, ok := status.FromError(err); ok {
		switch grpcStatus.Code() {
		case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied, codes.NotFound:
			return ClassPermanent
		case codes.Unknown, codes.OK:
			// Not a gRPC status error; fall through to the HTTP parsing below.
		default:
			return ClassTransient
		}
	}

	message := err.Error()
	// The OTLP/HTTP exporter retries 429/502/503/504 itself and reports the
	// exhausted retry as "retry-able request failure", without the status.
	if strings.Contains(message, "retry-able") {
		return ClassTransient
	}
	for _, phrase := range networkPhrases {
		if strings.Contains(message, phrase) {
			return ClassTransient
		}
	}
	if match := httpStatusPattern.FindStringSubmatch(message); match != nil {
		var code int
		if _, err := fmt.Sscanf(match[1], "%d", &code); err == nil {
			return classifyHTTPStatus(code)
		}
	}
	return ClassUnknown
}

// networkPhrases cover exporter wrappers that stringify a network failure
// instead of keeping the wrapped net.Error reachable.
var networkPhrases = []string{
	"connection refused",
	"connection reset",
	"broken pipe",
	"no such host",
	"i/o timeout",
	"tls handshake",
	"unexpected EOF",
}

func classifyHTTPStatus(code int) ErrorClass {
	switch code {
	case 400, 401, 403, 404, 405, 406, 410, 413, 414, 415, 422, 501, 505:
		return ClassPermanent
	default:
		return ClassTransient
	}
}

// GuardedExporter decorates an OpenTelemetry metric exporter.
type GuardedExporter struct {
	inner  metric.Exporter
	cfg    Config
	logger *zap.Logger

	mu sync.Mutex
	// state is the circuit state; stopped marks the permanent hard close
	// performed by Shutdown or HardShutdown.
	state   State
	stopped bool
	// failures counts consecutive failures; probing marks a half-open export
	// in flight, so a slow probe cannot be doubled.
	failures int
	probing  bool
	// alerted marks that the current outage already sent its alert.
	alerted     bool
	nextAttempt time.Time
	nextProbe   time.Time
	lastErr     error

	now          func() time.Time
	alerter      *NtfyAlerter
	shutdownOnce sync.Once
	shutdownErr  error
}

// Option customizes a GuardedExporter.
type Option func(*GuardedExporter)

// WithLogger sets the logger used for state changes and ntfy delivery problems.
func WithLogger(logger *zap.Logger) Option {
	return func(exporter *GuardedExporter) {
		if logger != nil {
			exporter.logger = logger
		}
	}
}

// New wraps inner. A zero Config is normalized to the defaults; an inner of nil
// is accepted but drops every export.
func New(inner metric.Exporter, cfg Config, options ...Option) *GuardedExporter {
	exporter := &GuardedExporter{
		inner:  inner,
		cfg:    cfg.normalized(),
		logger: zap.NewNop(),
		state:  StateClosed,
		now:    time.Now,
	}
	for _, option := range options {
		option(exporter)
	}
	exporter.alerter = NewNtfyAlerter(exporter.cfg.Ntfy, exporter.logger)
	if inner == nil {
		exporter.logger.Warn("otelguard: nil inner exporter, every export is dropped")
	}
	return exporter
}

// Config returns the normalized configuration in use.
func (exporter *GuardedExporter) Config() Config {
	return exporter.cfg
}

// State returns the current circuit state.
func (exporter *GuardedExporter) State() State {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	return exporter.state
}

// IsOpen reports whether the circuit is open (or hard closed). It is the
// predicate handed to NewSilentErrorHandler.
func (exporter *GuardedExporter) IsOpen() bool {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	return exporter.state == StateOpen || exporter.stopped
}

// LastError returns the most recent export failure, if any.
func (exporter *GuardedExporter) LastError() error {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	return exporter.lastErr
}

// Export delegates to the inner exporter when the circuit allows it and always
// returns nil: a metric export failure must never reach the SDK error handler,
// and the guard reports it through logs and ntfy instead.
func (exporter *GuardedExporter) Export(ctx context.Context, data *metricdata.ResourceMetrics) error {
	if exporter.cfg.Disabled {
		if exporter.inner == nil {
			return nil
		}
		return exporter.inner.Export(ctx, data)
	}
	if !exporter.allowExport() {
		return nil
	}

	err := exporter.inner.Export(ctx, data)
	exporter.recordResult(err)
	return nil
}

// Temporality delegates to the inner exporter so the reader sees the same
// temporality it would without the guard.
func (exporter *GuardedExporter) Temporality(kind metric.InstrumentKind) metricdata.Temporality {
	if exporter.inner == nil {
		return metricdata.CumulativeTemporality
	}
	return exporter.inner.Temporality(kind)
}

// Aggregation delegates to the inner exporter.
func (exporter *GuardedExporter) Aggregation(kind metric.InstrumentKind) metric.Aggregation {
	if exporter.inner == nil {
		return metric.AggregationDefault{}
	}
	return exporter.inner.Aggregation(kind)
}

// ForceFlush delegates to the inner exporter, except while the circuit is open
// or hard closed, where it returns nil immediately: there is no point waiting
// on an endpoint that just failed.
func (exporter *GuardedExporter) ForceFlush(ctx context.Context) error {
	if exporter.cfg.Disabled {
		if exporter.inner == nil {
			return nil
		}
		return exporter.inner.ForceFlush(ctx)
	}

	exporter.mu.Lock()
	skip := exporter.stopped || exporter.state == StateOpen
	inner := exporter.inner
	exporter.mu.Unlock()

	if skip || inner == nil {
		return nil
	}

	err := inner.ForceFlush(ctx)
	if err != nil {
		exporter.recordResult(err)
		return err
	}
	return nil
}

// Shutdown closes the guard and the inner exporter once. Afterwards the guard
// reports itself open and drops every export.
func (exporter *GuardedExporter) Shutdown(ctx context.Context) error {
	exporter.shutdownOnce.Do(func() {
		exporter.mu.Lock()
		exporter.stopped = true
		exporter.state = StateOpen
		exporter.probing = false
		exporter.mu.Unlock()

		if exporter.inner == nil {
			return
		}
		err := exporter.inner.Shutdown(ctx)

		exporter.mu.Lock()
		exporter.shutdownErr = err
		exporter.mu.Unlock()
	})

	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	return exporter.shutdownErr
}

// allowExport decides whether this cycle reaches the inner exporter. It never
// blocks: a closed window drops the export instead of delaying the reader.
func (exporter *GuardedExporter) allowExport() bool {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()

	if exporter.stopped || exporter.inner == nil {
		return false
	}

	now := exporter.now()
	switch exporter.state {
	case StateOpen:
		if now.Before(exporter.nextProbe) {
			return false
		}
		exporter.state = StateHalfOpen
		exporter.probing = true
		return true
	case StateHalfOpen:
		if exporter.probing {
			return false
		}
		exporter.probing = true
		return true
	default:
		return !now.Before(exporter.nextAttempt)
	}
}

// recordResult folds one export outcome into the circuit state and dispatches
// any alert off the export path.
func (exporter *GuardedExporter) recordResult(err error) {
	exporter.mu.Lock()
	exporter.probing = false

	var notification *Notification
	hardClose := false
	if err == nil {
		notification = exporter.onSuccessLocked()
	} else {
		notification, hardClose = exporter.onFailureLocked(err)
	}
	exporter.mu.Unlock()

	if hardClose {
		go exporter.hardShutdown()
	}
	if notification != nil {
		// Off the export path: a slow or unreachable ntfy must never delay an
		// export cycle. The alerter's client timeout bounds the goroutine.
		go exporter.alerter.Send(context.Background(), *notification)
	}
}

func (exporter *GuardedExporter) onSuccessLocked() *Notification {
	previous := exporter.state
	exporter.failures = 0
	exporter.nextAttempt = time.Time{}
	exporter.lastErr = nil
	exporter.state = StateClosed

	if previous != StateHalfOpen {
		return nil
	}

	exporter.alerted = false
	exporter.logger.Info("otelguard: metric export recovered",
		zap.String("service", exporter.cfg.Service),
		zap.String("endpoint", exporter.cfg.Endpoint))
	return ptr(exporter.recoveryNotification())
}

func (exporter *GuardedExporter) onFailureLocked(err error) (*Notification, bool) {
	class := Classify(err)
	exporter.failures++
	exporter.lastErr = err

	if exporter.failures >= exporter.cfg.MaxFailures {
		exporter.state = StateOpen
		exporter.probing = false
		exporter.nextProbe = exporter.now().Add(exporter.cfg.ProbeInterval)
		exporter.nextAttempt = time.Time{}

		// One alert per outage: a failed probe re-opens the circuit silently.
		if exporter.alerted {
			return nil, false
		}
		exporter.alerted = true
		exporter.logger.Error("otelguard: metric export circuit opened",
			zap.String("service", exporter.cfg.Service),
			zap.String("endpoint", exporter.cfg.Endpoint),
			zap.Int("failures", exporter.failures),
			zap.String("class", class.String()),
			zap.Duration("probe_interval", exporter.cfg.ProbeInterval),
			zap.Error(err))
		return ptr(exporter.alertNotification(class)), exporter.cfg.HardShutdown
	}

	exporter.state = StateClosed
	delay := exporter.backoff(exporter.failures)
	exporter.nextAttempt = exporter.now().Add(delay)
	exporter.logger.Debug("otelguard: metric export failed, backing off",
		zap.Int("failures", exporter.failures),
		zap.String("class", class.String()),
		zap.Duration("next_attempt_in", delay),
		zap.Error(err))
	return nil, false
}

// backoff grows 2s → 4s → 8s … under MaxBackoff, with relative jitter.
func (exporter *GuardedExporter) backoff(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	delay := float64(exporter.cfg.InitialBackoff) * math.Pow(exporter.cfg.Multiplier, float64(failures-1))
	if maxDelay := float64(exporter.cfg.MaxBackoff); delay > maxDelay {
		delay = maxDelay
	}
	if exporter.cfg.Jitter > 0 {
		delay += delay * exporter.cfg.Jitter * (rand.Float64()*2 - 1)
	}
	if delay < 0 {
		return 0
	}
	return time.Duration(delay)
}

// hardShutdown implements Config.HardShutdown: the open circuit gives up on the
// inner exporter instead of probing it forever.
func (exporter *GuardedExporter) hardShutdown() {
	timeout := exporter.cfg.ShutdownTimeout
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := exporter.Shutdown(ctx); err != nil {
		exporter.logger.Warn("otelguard: hard shutdown of inner exporter failed", zap.Error(err))
	}
}

func (exporter *GuardedExporter) alertNotification(class ErrorClass) Notification {
	subject := exporter.subject()
	return Notification{
		Title:    "指标导出熔断：" + subject,
		Priority: exporter.cfg.Ntfy.AlertPriority,
		Tags:     exporter.cfg.Ntfy.AlertTags,
		Message: fmt.Sprintf("%s 的 OTLP 指标导出连续失败 %d 次，已熔断。\n错误类型：%s\n%s\n端点：%s\n最近错误：%s\n恢复探测间隔：%s（探测成功后自动恢复）",
			subject, exporter.failures, class.String(), adviceFor(class),
			orUnknown(exporter.cfg.Endpoint), truncate(exporter.lastErr, 400), exporter.cfg.ProbeInterval),
	}
}

func (exporter *GuardedExporter) recoveryNotification() Notification {
	return Notification{
		Title:    "指标导出恢复：" + exporter.subject(),
		Priority: exporter.cfg.Ntfy.RecoveryPriority,
		Tags:     exporter.cfg.Ntfy.RecoveryTags,
		Message: fmt.Sprintf("%s 的 OTLP 指标导出已恢复，熔断器回到 closed。\n端点：%s",
			exporter.subject(), orUnknown(exporter.cfg.Endpoint)),
	}
}

func (exporter *GuardedExporter) subject() string {
	if exporter.cfg.Service != "" {
		return exporter.cfg.Service
	}
	return "未命名服务"
}

func adviceFor(class ErrorClass) string {
	if class == ClassPermanent {
		return "排查建议：鉴权或配置错误，检查 OTLP 端点、Token 与路径"
	}
	return "排查建议：后端不可用或网络故障，确认端点可达后等待自动恢复"
}

func orUnknown(value string) string {
	if value == "" {
		return "(未配置)"
	}
	return value
}

func truncate(err error, limit int) string {
	if err == nil {
		return "(无)"
	}
	message := err.Error()
	if len(message) <= limit {
		return message
	}
	return message[:limit] + "…"
}

func ptr[T any](value T) *T {
	return &value
}
