package config

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type stubWatchSource struct {
	events  chan WatchEvent
	started chan struct{}
}

func newStubWatchSource() *stubWatchSource {
	return &stubWatchSource{
		events:  make(chan WatchEvent),
		started: make(chan struct{}, 1),
	}
}

func (source *stubWatchSource) Name() string { return "stub" }
func (source *stubWatchSource) Load(context.Context) (map[string]any, error) {
	return map[string]any{"value": "initial"}, nil
}
func (source *stubWatchSource) Watch(ctx context.Context, onEvent func(WatchEvent)) error {
	source.started <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-source.events:
			onEvent(event)
		}
	}
}

func runStartWatch(t *testing.T, live *Live[*wrapperspb.StringValue]) *stubWatchSource {
	t.Helper()
	source := newStubWatchSource()
	live.source = source
	lifecycle := fxtest.NewLifecycle(t)
	startWatch(lifecycle, zap.NewNop(), live, nil)
	lifecycle.RequireStart()
	t.Cleanup(lifecycle.RequireStop)
	select {
	case <-source.started:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch was not started")
	}
	return source
}

func (source *stubWatchSource) send(t *testing.T, event WatchEvent) {
	t.Helper()
	select {
	case source.events <- event:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out sending watch event")
	}
}

func (source *stubWatchSource) barrier(t *testing.T) {
	t.Helper()
	source.send(t, WatchEvent{Err: errors.New("barrier")})
}

func TestStartWatchAppliesValidUpdate(t *testing.T) {
	live := NewLive(wrapperspb.String("old"))
	source := runStartWatch(t, live)
	applied := make(chan struct{})
	live.Subscribe(func(_, _ *wrapperspb.StringValue) { close(applied) })

	source.send(t, WatchEvent{Raw: map[string]any{"value": "new"}})
	select {
	case <-applied:
	case <-time.After(2 * time.Second):
		t.Fatal("valid update was not applied")
	}
	assert.Equal(t, "new", live.Get().GetValue())
}

func TestStartWatchKeepsLastKnownGood(t *testing.T) {
	tests := []struct {
		name      string
		event     WatchEvent
		validator func(*wrapperspb.StringValue) error
	}{
		{name: "event error", event: WatchEvent{Err: errors.New("bad event")}},
		{name: "deleted", event: WatchEvent{Deleted: true}},
		{name: "decode failure", event: WatchEvent{Raw: map[string]any{"unknown": true}}},
		{
			name:  "validation failure",
			event: WatchEvent{Raw: map[string]any{"value": "rejected"}},
			validator: func(*wrapperspb.StringValue) error {
				return errors.New("invalid")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			live := NewLive(wrapperspb.String("keep"))
			if test.validator != nil {
				live.validate = test.validator
			}
			source := runStartWatch(t, live)
			source.send(t, test.event)
			source.barrier(t)
			assert.Equal(t, "keep", live.Get().GetValue())
		})
	}
}

type plainSource struct{}

func (plainSource) Name() string { return "plain" }
func (plainSource) Load(context.Context) (map[string]any, error) {
	return map[string]any{"value": "initial"}, nil
}

func TestStartWatchDiscoversOptionalWatcherAtRuntime(t *testing.T) {
	live := NewLive(wrapperspb.String("initial"))
	live.source = plainSource{}
	lifecycle := fxtest.NewLifecycle(t)
	require.NotPanics(t, func() { startWatch(lifecycle, zap.NewNop(), live, nil) })
	lifecycle.RequireStart()
	lifecycle.RequireStop()
	assert.Equal(t, "initial", live.Get().GetValue())
}
