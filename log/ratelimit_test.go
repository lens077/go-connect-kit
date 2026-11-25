package log

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func rateLimitedLogger(opts RateLimitOptions, now *time.Time) (*zap.Logger, *observer.ObservedLogs, *rateLimitCore) {
	core, logs := observer.New(zapcore.DebugLevel)
	wrapped := newRateLimitCore(core, opts).(*rateLimitCore)
	if now != nil {
		wrapped.now = func() time.Time { return *now }
	}
	return zap.New(wrapped), logs, wrapped
}

func TestRateLimitDropsRepeatsWithinWindow(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	logger, logs, _ := rateLimitedLogger(RateLimitOptions{Interval: time.Minute, Burst: 2}, &now)

	for i := 0; i < 5; i++ {
		logger.Info("same message")
	}
	require.Equal(t, 2, logs.Len())
	assert.Equal(t, "same message", logs.AllUntimed()[0].Message)
}

func TestRateLimitEmitsSuppressedSummaryOnRollover(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	logger, logs, _ := rateLimitedLogger(RateLimitOptions{Interval: time.Minute, Burst: 1}, &now)

	logger.Info("same message")
	logger.Info("same message")
	logger.Info("same message")
	require.Equal(t, 1, logs.Len())

	now = now.Add(2 * time.Minute)
	logger.Info("same message")

	require.Equal(t, 3, logs.Len())
	entries := logs.AllUntimed()
	assert.Equal(t, "same message", entries[0].Message)
	assert.Equal(t, zapcore.WarnLevel, entries[1].Level)
	assert.True(t, strings.Contains(entries[1].Message, "suppressed 2 messages"), entries[1].Message)
	assert.Equal(t, "same message", entries[2].Message)
}

func TestRateLimitSeparatesLevelsAndMessages(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	logger, logs, _ := rateLimitedLogger(RateLimitOptions{Interval: time.Minute, Burst: 1}, &now)

	logger.Info("one")
	logger.Info("two")
	logger.Error("one")
	require.Equal(t, 3, logs.Len())
}

func TestRateLimitStateIsSharedThroughWith(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	logger, logs, _ := rateLimitedLogger(RateLimitOptions{Interval: time.Minute, Burst: 1}, &now)

	derived := logger.With(zap.String("component", "derived"))
	logger.Info("same message")
	derived.Info("same message")
	require.Equal(t, 1, logs.Len())
}

func TestRateLimitIsConcurrencySafe(t *testing.T) {
	logger, logs, _ := rateLimitedLogger(RateLimitOptions{Interval: time.Minute, Burst: 10}, nil)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("concurrent message")
		}()
	}
	wg.Wait()
	assert.LessOrEqual(t, logs.Len(), 10)
	assert.Greater(t, logs.Len(), 0)
}

func TestRateLimitDoesNotChangeLevelFiltering(t *testing.T) {
	core, _ := observer.New(zapcore.ErrorLevel)
	wrapped := newRateLimitCore(core, RateLimitOptions{})
	assert.False(t, wrapped.Enabled(zapcore.InfoLevel))
	assert.True(t, wrapped.Enabled(zapcore.ErrorLevel))
}
