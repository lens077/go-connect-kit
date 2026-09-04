package otel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/lens077/go-connect-kit/meta"
	redisotel "github.com/redis/go-redis/extra/redisotel-native/v9"
	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	defaultSampleRatio    = 1.0
	defaultMetricInterval = 30 * time.Second
)

// TLSOptions is the provider-neutral TLS configuration shared by OTLP signals.
type TLSOptions struct {
	Enabled            bool
	InsecureSkipVerify bool
	CAPEM              string
}

// TraceOptions configures OTLP trace export.
type TraceOptions struct {
	Endpoint    string
	SampleRatio *float64
	TLS         TLSOptions
}

// MetricOptions configures OTLP metric export.
type MetricOptions struct {
	Endpoint       string
	ExportInterval time.Duration
	TLS            TLSOptions
}

// LoggingOptions configures OTLP log export.
type LoggingOptions struct {
	Endpoint string
	TLS      TLSOptions
}

// Options composes only the signals a caller needs. A nil signal is disabled.
type Options struct {
	Trace            *TraceOptions
	Metric           *MetricOptions
	Logging          *LoggingOptions
	ServiceNamespace string
	RuntimeMetrics   bool
}

// Module provides the unified OpenTelemetry shutdown function.
var Module = fx.Module("otel",
	fx.Provide(func(info meta.AppInfo, options Options, logger *zap.Logger) (func(context.Context) error, error) {
		return SetupOTelSDK(context.Background(), info, options, logger)
	}),
)

// SetupOTelSDK initializes the configured trace, metric, and log export pipelines.
func SetupOTelSDK(ctx context.Context, info meta.AppInfo, options Options, logger *zap.Logger) (func(context.Context) error, error) {
	if options.Trace == nil && options.Metric == nil && options.Logging == nil {
		logger.Info("observability is disabled, skipping OpenTelemetry setup")
		return func(context.Context) error { return nil }, nil
	}

	var (
		shutdownMu    sync.Mutex
		shutdownFuncs []func(context.Context) error
	)
	shutdown := func(ctx context.Context) error {
		shutdownMu.Lock()
		funcs := shutdownFuncs
		shutdownFuncs = nil
		shutdownMu.Unlock()

		var err error
		for index := len(funcs) - 1; index >= 0; index-- {
			err = errors.Join(err, funcs[index](ctx))
		}
		return err
	}

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Error("opentelemetry sdk error", zap.Error(err))
	}))
	otel.SetTextMapPropagator(newPropagator())

	res, err := newResource(info, options.ServiceNamespace)
	if err != nil {
		return shutdown, errors.Join(err, shutdown(ctx))
	}

	if options.Trace != nil {
		tracerProvider, err := newTracerProvider(ctx, res, *options.Trace, logger)
		if err != nil {
			return shutdown, errors.Join(err, shutdown(ctx))
		}
		shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
		otel.SetTracerProvider(tracerProvider)
	}

	if options.Metric != nil {
		meterProvider, err := newMeterProvider(ctx, res, *options.Metric, options.RuntimeMetrics, logger)
		if err != nil {
			return shutdown, errors.Join(err, shutdown(ctx))
		}
		shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
		otel.SetMeterProvider(meterProvider)

		if options.RuntimeMetrics {
			if err := otelruntime.Start(); err != nil {
				return shutdown, errors.Join(err, shutdown(ctx))
			}
		}
	}

	if options.Logging != nil {
		loggerProvider, err := newLoggerProvider(ctx, res, *options.Logging, logger)
		if err != nil {
			return shutdown, errors.Join(err, shutdown(ctx))
		}
		shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
		global.SetLoggerProvider(loggerProvider)
	}

	return shutdown, nil
}

func tlsClientConfig(options TLSOptions, logger *zap.Logger) *tls.Config {
	if !options.Enabled {
		return nil
	}

	config := &tls.Config{InsecureSkipVerify: options.InsecureSkipVerify}
	if !options.InsecureSkipVerify && options.CAPEM != "" {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM([]byte(options.CAPEM)) {
			config.RootCAs = pool
		} else {
			logger.Error("failed to append ca cert, falling back to system roots")
		}
	}
	return config
}

func newResource(info meta.AppInfo, namespace string) (*resource.Resource, error) {
	otelAttributes := []attribute.KeyValue{
		semconv.ServiceName(info.Name),
		semconv.ServiceVersion(info.Version),
		semconv.AppBuildID(meta.Version),
		semconv.ServiceInstanceID(info.ID),
		semconv.DeploymentEnvironmentNameKey.String(info.Environment),
		semconv.HostIP(info.Host),
		semconv.ProcessRuntimeName("go"),
		semconv.ProcessRuntimeVersion(runtime.Version()),
	}
	if namespace != "" {
		otelAttributes = append(otelAttributes, semconv.ServiceNamespace(namespace))
	}
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, otelAttributes...),
	)
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

var sqlcQueryNameRE = regexp.MustCompile(`--\s*name:\s*(\S+)`)

// SQLSpanName returns a bounded, useful span name for sqlc and handwritten SQL.
func SQLSpanName(statement string) string {
	if match := sqlcQueryNameRE.FindStringSubmatch(statement); match != nil {
		return match[1]
	}

	for line := range strings.SplitSeq(statement, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		if index := strings.IndexAny(line, " \t("); index > 0 {
			return line[:index]
		}
		return line
	}
	return "unknown"
}

func sampleRatio(options TraceOptions, logger *zap.Logger) float64 {
	if options.SampleRatio == nil {
		return defaultSampleRatio
	}

	ratio := *options.SampleRatio
	switch {
	case ratio < 0:
		logger.Warn("trace.sample_ratio below 0, clamped to 0", zap.Float64("configured", ratio))
		return 0
	case ratio > 1:
		logger.Warn("trace.sample_ratio above 1, clamped to 1", zap.Float64("configured", ratio))
		return 1
	default:
		return ratio
	}
}

func newTracerProvider(ctx context.Context, res *resource.Resource, options TraceOptions, logger *zap.Logger) (*trace.TracerProvider, error) {
	exporterOptions := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(options.Endpoint),
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
	}
	if tlsConfig := tlsClientConfig(options.TLS, logger); tlsConfig != nil {
		exporterOptions = append(exporterOptions, otlptracehttp.WithTLSClientConfig(tlsConfig))
	} else {
		exporterOptions = append(exporterOptions, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(ctx, exporterOptions...)
	if err != nil {
		return nil, err
	}

	ratio := sampleRatio(options, logger)
	logger.Info("trace sampler configured", zap.Float64("ratio", ratio))
	return trace.NewTracerProvider(
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(ratio))),
		trace.WithResource(res),
		trace.WithSpanProcessor(trace.NewBatchSpanProcessor(exporter)),
	), nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource, options MetricOptions, runtimeMetrics bool, logger *zap.Logger) (*metric.MeterProvider, error) {
	exporterOptions := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(options.Endpoint),
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
	}
	if tlsConfig := tlsClientConfig(options.TLS, logger); tlsConfig != nil {
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithTLSClientConfig(tlsConfig))
	} else {
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithInsecure())
	}

	exporter, err := otlpmetrichttp.New(ctx, exporterOptions...)
	if err != nil {
		return nil, err
	}

	interval := options.ExportInterval
	if interval <= 0 {
		interval = defaultMetricInterval
	}
	readerOptions := []metric.PeriodicReaderOption{metric.WithInterval(interval)}
	if runtimeMetrics {
		readerOptions = append(readerOptions, metric.WithProducer(otelruntime.NewProducer()))
	}
	logger.Info("metric exporter configured", zap.Duration("interval", interval))
	return metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exporter, readerOptions...)),
	), nil
}

var redisOTelOnce sync.Once

// EnsureRedisInstrumentation enables go-redis metrics once per process.
func EnsureRedisInstrumentation(logger *zap.Logger) {
	redisOTelOnce.Do(func() {
		config := redisotel.NewConfig().WithEnabled(true)
		if err := redisotel.GetObservabilityInstance().Init(config); err != nil {
			logger.Error("failed to init redis otel instrumentation", zap.Error(err))
		}
	})
}

func newLoggerProvider(ctx context.Context, res *resource.Resource, options LoggingOptions, logger *zap.Logger) (*sdklog.LoggerProvider, error) {
	exporterOptions := []otlploghttp.Option{
		otlploghttp.WithEndpoint(options.Endpoint),
		otlploghttp.WithCompression(otlploghttp.GzipCompression),
	}
	if tlsConfig := tlsClientConfig(options.TLS, logger); tlsConfig != nil {
		exporterOptions = append(exporterOptions, otlploghttp.WithTLSClientConfig(tlsConfig))
	} else {
		exporterOptions = append(exporterOptions, otlploghttp.WithInsecure())
	}

	exporter, err := otlploghttp.New(ctx, exporterOptions...)
	if err != nil {
		return nil, err
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	), nil
}
