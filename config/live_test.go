package config

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestLiveReturnsLatest(t *testing.T) {
	live := NewLive(wrapperspb.String("old"))
	assert.Equal(t, "old", live.Get().GetValue())

	live.Set(wrapperspb.String("new"))
	assert.Equal(t, "new", live.Get().GetValue())
}

func TestLiveNormalizesTypedNil(t *testing.T) {
	var nilMessage *wrapperspb.StringValue
	live := NewLive(nilMessage)
	require.NotNil(t, live.Get())

	live.Set(wrapperspb.String("kept"))
	live.Set(nilMessage)
	assert.Equal(t, "kept", live.Get().GetValue())
}

func TestLiveSubscriberSeesSwappedValue(t *testing.T) {
	live := NewLive(wrapperspb.String("old"))
	var oldValue, currentValue, observedValue string
	live.Subscribe(func(old, current *wrapperspb.StringValue) {
		oldValue = old.GetValue()
		currentValue = current.GetValue()
		observedValue = live.Get().GetValue()
	})

	live.Set(wrapperspb.String("new"))
	assert.Equal(t, "old", oldValue)
	assert.Equal(t, "new", currentValue)
	assert.Equal(t, "new", observedValue)
}

func TestLiveSubscribeCancelIsIdempotent(t *testing.T) {
	live := NewLive(wrapperspb.String("initial"))
	calls := 0
	cancel := live.Subscribe(func(_, _ *wrapperspb.StringValue) { calls++ })

	live.Set(wrapperspb.String("once"))
	require.Equal(t, 1, calls)
	cancel()
	assert.NotPanics(t, cancel)
	live.Set(wrapperspb.String("twice"))
	assert.Equal(t, 1, calls)
}

func TestLiveCallbacksAreSerialized(t *testing.T) {
	live := NewLive(wrapperspb.String("initial"))
	var mu sync.Mutex
	inFlight := 0
	maxInFlight := 0
	live.Subscribe(func(_, _ *wrapperspb.StringValue) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			inFlight--
			mu.Unlock()
		}()
	})

	var wait sync.WaitGroup
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			live.Set(wrapperspb.String("updated"))
		}()
	}
	wait.Wait()
	assert.Equal(t, 1, maxInFlight)
}
