// Command example wires otelguard into a real OTLP metric pipeline so the
// guard behaviour can be observed without touching a service.
//
// Point it at an endpoint that rejects or drops requests and watch the state
// transition Closed → Open and, once the endpoint comes back, HalfOpen →
// Closed:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT=otlp-dev.apikv.com:443 \
//	OTEL_GUARD_NTFY_URL=https://ntfy.example.com OTEL_GUARD_NTFY_TOPIC=otel-guard \
//	go run ./otelguard/example
//
// The same wiring lives inside go-connect-kit/otel: set MetricOptions.Guard and
// SetupOTelSDK installs the guard and the silent error handler for you.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lens077/go-connect-kit/otelguard"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("build logger: %v", err)
	}
	defer func() { _ = logger.Sync() }()

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:4318"
	}

	inner, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(endpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		logger.Fatal("build OTLP exporter", zap.Error(err))
	}

	// ConfigFromEnv carries the ntfy URL, topic and token from the deployment;
	// the demo thresholds below only apply when the environment is silent.
	guardConfig := otelguard.ConfigFromEnv()
	guardConfig.Service = "otelguard-example"
	guardConfig.Endpoint = endpoint
	if guardConfig.MaxFailures == otelguard.DefaultMaxFailures {
		guardConfig.MaxFailures = 3
		guardConfig.InitialBackoff = time.Second
		guardConfig.ProbeInterval = 5 * time.Second
	}

	guarded := otelguard.New(inner, guardConfig, otelguard.WithLogger(logger))

	reader := sdkmetric.NewPeriodicReader(guarded, sdkmetric.WithInterval(2*time.Second))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	otel.SetErrorHandler(otelguard.NewSilentErrorHandler(guarded.IsOpen, logger))

	meter := provider.Meter("example")
	counter, err := meter.Int64Counter("example.exports")
	if err != nil {
		logger.Fatal("create counter", zap.Error(err))
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for tick := 1; tick <= 30; tick++ {
		counter.Add(ctx, 1)
		logger.Info("tick",
			zap.Int("tick", tick),
			zap.String("guard_state", guarded.State().String()))
		if guarded.IsOpen() {
			logger.Info("guard is open: exports are dropped, no error log is printed")
		}

		select {
		case <-ctx.Done():
			tick = 31
		case <-ticker.C:
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		logger.Warn("meter provider shutdown", zap.Error(err))
	}
}
