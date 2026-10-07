package otelguard

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// SilentErrorHandler is the global OpenTelemetry error handler that keeps a
// broken export pipeline out of the business log. While the guarded exporter is
// open it drops SDK errors entirely; otherwise it logs at most one warning per
// interval.
//
// Installing it is what stops `metric.PeriodicReader` from reporting the same
// export failure on every collection cycle.
type SilentErrorHandler struct {
	isOpen   func() bool
	logger   *zap.Logger
	interval time.Duration

	mu       sync.Mutex
	lastWarn time.Time
}

// NewSilentErrorHandler builds the handler. isOpen may be nil (never silent).
// interval defaults to DefaultErrorLogInterval.
func NewSilentErrorHandler(isOpen func() bool, logger *zap.Logger, interval ...time.Duration) *SilentErrorHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	window := DefaultErrorLogInterval
	if len(interval) > 0 && interval[0] > 0 {
		window = interval[0]
	}
	return &SilentErrorHandler{isOpen: isOpen, logger: logger, interval: window}
}

// Handle implements otel.ErrorHandler.
func (handler *SilentErrorHandler) Handle(err error) {
	if handler == nil || err == nil {
		return
	}
	if handler.isOpen != nil && handler.isOpen() {
		return
	}

	handler.mu.Lock()
	now := time.Now()
	if !handler.lastWarn.IsZero() && now.Sub(handler.lastWarn) < handler.interval {
		handler.mu.Unlock()
		return
	}
	handler.lastWarn = now
	handler.mu.Unlock()

	handler.logger.Warn("otelguard: OpenTelemetry SDK error", zap.Error(err))
}
