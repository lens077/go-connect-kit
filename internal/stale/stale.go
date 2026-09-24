// Package stale records that a hot-reloadable resource did not apply the latest configuration.
//
// Hot reload keeps the previous connection when a rebuild with new configuration fails, so
// traffic keeps flowing. Without this record that state is only visible as one ERROR line:
// the configuration source shows the new version, health checks stay green, and the bad
// configuration surfaces only at the next restart, far from the change that caused it
// (2026-09-24 ecommerce hot-reload test).
package stale

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetricName is the gauge that is 1 while a component runs on configuration older than the
// latest push, and 0 otherwise.
const MetricName = "connectkit.config.stale"

// State is safe for concurrent use. The zero value means "up to date".
type State struct {
	err atomic.Pointer[error]
}

// Set records why the latest configuration is not in use; nil clears it.
func (s *State) Set(err error) {
	if err == nil {
		s.err.Store(nil)
		return
	}
	s.err.Store(&err)
}

// Err returns the recorded reason, or nil when the component matches the latest configuration.
func (s *State) Err() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}

var registry struct {
	sync.RWMutex
	states map[string]*State
	once   sync.Once
	err    error
}

// Register exports the state as MetricName with a component attribute. It uses the global
// meter provider, which delegates to the real provider once the OTel module installs it.
//
// The metric instrument is process-global and must be created once. Registering the same
// observable gauge name separately for pgpool and redisclient causes the real SDK to keep
// only the first callback, even though the no-op global provider accepts both registrations.
func Register(component string, state *State) error {
	registry.Lock()
	if registry.states == nil {
		registry.states = make(map[string]*State)
	}
	registry.states[component] = state
	registry.Unlock()

	registry.once.Do(func() {
		meter := otel.Meter("github.com/lens077/go-connect-kit")
		_, registry.err = meter.Int64ObservableGauge(MetricName,
			metric.WithDescription("1 while the component keeps its previous connection because rebuilding with the latest configuration failed"),
			metric.WithInt64Callback(observe),
		)
	})
	if registry.err != nil {
		return fmt.Errorf("register %s gauge: %w", MetricName, registry.err)
	}
	return nil
}

func observe(_ context.Context, observer metric.Int64Observer) error {
	registry.RLock()
	defer registry.RUnlock()
	for component, state := range registry.states {
		var value int64
		if state.Err() != nil {
			value = 1
		}
		observer.Observe(value, metric.WithAttributes(attribute.String("component", component)))
	}
	return nil
}
