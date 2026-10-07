package otelguard

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Defaults for Config. A zero Config is normalized to these values by New,
// except for Jitter, where 0 is honored as "no jitter" (use DefaultConfig to
// get the 0.3 default).
const (
	DefaultMaxFailures      = 8
	DefaultInitialBackoff   = 2 * time.Second
	DefaultMaxBackoff       = 5 * time.Minute
	DefaultMultiplier       = 2.0
	DefaultJitter           = 0.3
	DefaultProbeInterval    = 5 * time.Minute
	DefaultErrorLogInterval = 30 * time.Second
	DefaultRequestTimeout   = 5 * time.Second
	DefaultShutdownTimeout  = 10 * time.Second
)

// Config controls the guard: when it opens, when it probes, and where alerts go.
type Config struct {
	// Disabled bypasses the guard entirely: Export delegates to the inner
	// exporter and returns its error unchanged. Wiring layers may use it to
	// keep a guard configurable without unwrapping the exporter.
	Disabled bool

	// Service and Endpoint only decorate log lines and alerts.
	Service  string
	Endpoint string

	// MaxFailures is the number of consecutive failures that opens the circuit.
	MaxFailures int
	// InitialBackoff is the skip window after the first failure.
	InitialBackoff time.Duration
	// MaxBackoff caps the skip window.
	MaxBackoff time.Duration
	// Multiplier grows the skip window per consecutive failure.
	Multiplier float64
	// Jitter is the relative randomness applied to the skip window, in [0, 1).
	Jitter float64
	// ProbeInterval is how long the open circuit waits before one trial export.
	ProbeInterval time.Duration
	// HardShutdown calls the inner exporter's Shutdown when the circuit opens.
	// No probe follows; the guard stays closed for good.
	HardShutdown bool
	// ShutdownTimeout bounds the Shutdown call made by HardShutdown.
	ShutdownTimeout time.Duration
	// ErrorLogInterval rate-limits the global OTel error handler while closed.
	ErrorLogInterval time.Duration

	Ntfy NtfyConfig
}

// DefaultConfig returns the production defaults from the design doc.
func DefaultConfig() Config {
	return Config{
		MaxFailures:      DefaultMaxFailures,
		InitialBackoff:   DefaultInitialBackoff,
		MaxBackoff:       DefaultMaxBackoff,
		Multiplier:       DefaultMultiplier,
		Jitter:           DefaultJitter,
		ProbeInterval:    DefaultProbeInterval,
		ShutdownTimeout:  DefaultShutdownTimeout,
		ErrorLogInterval: DefaultErrorLogInterval,
		Ntfy:             DefaultNtfyConfig(),
	}
}

// ConfigFromEnv returns DefaultConfig overridden by OTEL_GUARD_* environment
// variables. Callers opt in by calling it; the package never reads the
// environment on its own.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()
	cfg.Disabled = envBool("DISABLED", cfg.Disabled)
	cfg.Service = envString("SERVICE", cfg.Service)
	cfg.Endpoint = envString("ENDPOINT", cfg.Endpoint)
	cfg.MaxFailures = envInt("MAX_FAILURES", cfg.MaxFailures)
	cfg.InitialBackoff = envDuration("INITIAL_BACKOFF", cfg.InitialBackoff)
	cfg.MaxBackoff = envDuration("MAX_BACKOFF", cfg.MaxBackoff)
	cfg.Multiplier = envFloat("MULTIPLIER", cfg.Multiplier)
	cfg.Jitter = envFloat("JITTER", cfg.Jitter)
	cfg.ProbeInterval = envDuration("PROBE_INTERVAL", cfg.ProbeInterval)
	cfg.HardShutdown = envBool("HARD_SHUTDOWN", cfg.HardShutdown)
	cfg.ShutdownTimeout = envDuration("SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout)
	cfg.ErrorLogInterval = envDuration("ERROR_LOG_INTERVAL", cfg.ErrorLogInterval)

	cfg.Ntfy.URL = envString("NTFY_URL", cfg.Ntfy.URL)
	cfg.Ntfy.Topic = envString("NTFY_TOPIC", cfg.Ntfy.Topic)
	cfg.Ntfy.Token = envString("NTFY_TOKEN", cfg.Ntfy.Token)
	cfg.Ntfy.AlertPriority = envString("NTFY_ALERT_PRIORITY", cfg.Ntfy.AlertPriority)
	cfg.Ntfy.RecoveryPriority = envString("NTFY_RECOVERY_PRIORITY", cfg.Ntfy.RecoveryPriority)
	return cfg
}

// normalized fills unset fields and clamps values into a usable range.
func (cfg Config) normalized() Config {
	if cfg.MaxFailures < 1 {
		cfg.MaxFailures = DefaultMaxFailures
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = DefaultInitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultMaxBackoff
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		cfg.MaxBackoff = cfg.InitialBackoff
	}
	if cfg.Multiplier < 1 {
		cfg.Multiplier = DefaultMultiplier
	}
	switch {
	case cfg.Jitter < 0:
		cfg.Jitter = 0
	case cfg.Jitter >= 1:
		cfg.Jitter = 0.9
	}
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = DefaultProbeInterval
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	if cfg.ErrorLogInterval <= 0 {
		cfg.ErrorLogInterval = DefaultErrorLogInterval
	}
	cfg.Ntfy = cfg.Ntfy.normalized()
	return cfg
}

const envPrefix = "OTEL_GUARD_"

func envString(name, fallback string) string {
	if value, ok := os.LookupEnv(envPrefix + name); ok && value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	if value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(envPrefix + name))); err == nil {
		return value
	}
	return fallback
}

func envFloat(name string, fallback float64) float64 {
	if value, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv(envPrefix+name)), 64); err == nil {
		return value
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	if value, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(envPrefix + name))); err == nil {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(envPrefix + name))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return value
}
