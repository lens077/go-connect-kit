package log

import (
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

const (
	DefaultRateLimitInterval = 10 * time.Second
	DefaultRateLimitBurst    = 20
)

// RateLimitOptions configures same-message rate limiting. Passing a non-nil
// pointer in Options.LogRateLimit enables it; zero-valued fields get the
// defaults above.
type RateLimitOptions struct {
	Interval time.Duration
	Burst    int
}

func (options RateLimitOptions) normalized() (time.Duration, int) {
	interval := options.Interval
	if interval <= 0 {
		interval = DefaultRateLimitInterval
	}
	burst := options.Burst
	if burst <= 0 {
		burst = DefaultRateLimitBurst
	}
	return interval, burst
}

type rateLimitState struct {
	mu         sync.Mutex
	count      int
	suppressed int
	windowEnd  time.Time
}

// rateLimitCore drops repeated messages within a fixed window and emits one
// summary entry when the window rolls over. State is shared by all cores
// derived through With so derived loggers do not get their own budget.
type rateLimitCore struct {
	zapcore.Core
	states   *sync.Map // map[string]*rateLimitState
	interval time.Duration
	burst    int
	now      func() time.Time
}

func newRateLimitCore(core zapcore.Core, opts RateLimitOptions) zapcore.Core {
	interval, burst := opts.normalized()
	return &rateLimitCore{
		Core:     core,
		states:   &sync.Map{},
		interval: interval,
		burst:    burst,
		now:      time.Now,
	}
}

func (core *rateLimitCore) Enabled(level zapcore.Level) bool {
	return core.Core.Enabled(level)
}

func (core *rateLimitCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if !core.Enabled(entry.Level) {
		return checked
	}
	return checked.AddCore(entry, core)
}

func (core *rateLimitCore) With(fields []zapcore.Field) zapcore.Core {
	return &rateLimitCore{
		Core:     core.Core.With(fields),
		states:   core.states,
		interval: core.interval,
		burst:    core.burst,
		now:      core.now,
	}
}

func (core *rateLimitCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	// Level + message keeps an error and an info with identical text in
	// separate buckets without ever storing field values (which may be secret).
	key := entry.Level.String() + "|" + entry.Message

	state, _ := core.states.LoadOrStore(key, &rateLimitState{})
	bucket := state.(*rateLimitState)

	bucket.mu.Lock()
	now := core.now()
	pending := 0
	if now.After(bucket.windowEnd) {
		pending = bucket.suppressed
		bucket.suppressed = 0
		bucket.count = 0
		bucket.windowEnd = now.Add(core.interval)
	}
	allowed := bucket.count < core.burst
	if allowed {
		bucket.count++
	} else {
		bucket.suppressed++
	}
	bucket.mu.Unlock()

	// Write outside the lock: a slow sink must not block other levels/messages.
	if pending > 0 {
		core.emitSuppressed(key, pending)
	}
	if !allowed {
		return nil
	}
	return core.Core.Write(entry, fields)
}

func (core *rateLimitCore) emitSuppressed(key string, count int) {
	// Deliberately bypass this core's own Write to avoid recursively feeding
	// the summary back into the limiter.
	_ = core.Core.Write(zapcore.Entry{
		Level:      zapcore.WarnLevel,
		Time:       core.now(),
		LoggerName: "log.ratelimit",
		Message:    "log rate limit: suppressed " + strconv.Itoa(count) + " messages for " + key,
	}, nil)
}
