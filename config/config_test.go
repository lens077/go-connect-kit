package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type staticSource struct {
	name string
	raw  map[string]any
	err  error
}

func (source staticSource) Name() string { return source.name }
func (source staticSource) Load(context.Context) (map[string]any, error) {
	return source.raw, source.err
}

func TestNewLoadsGenericMessageAndTracksSource(t *testing.T) {
	live, err := New[*wrapperspb.StringValue](staticSource{
		name: "static",
		raw:  map[string]any{"value": "loaded"},
	})
	require.NoError(t, err)
	assert.Equal(t, "loaded", live.Get().GetValue())
	assert.Equal(t, "static", live.SourceName())
}

func TestNewRejectsUnknownKeys(t *testing.T) {
	live, err := New[*wrapperspb.StringValue](staticSource{
		name: "static",
		raw:  map[string]any{"unexpected": true},
	})
	assert.Nil(t, live)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode config from static")
}

func TestNewWithOptionsCanPreserveCallerDecodePolicy(t *testing.T) {
	live, err := NewWithOptions[*wrapperspb.StringValue](context.Background(), staticSource{
		name: "static",
		raw:  map[string]any{"unexpected": true},
	}, LoadOptions{AllowUnknownFields: true, SkipValidation: true})
	require.NoError(t, err)
	assert.Empty(t, live.Get().GetValue())
}

func TestNewRejectsNilSource(t *testing.T) {
	live, err := New[*wrapperspb.StringValue](nil)
	assert.Nil(t, live)
	require.EqualError(t, err, "config source is nil")
}
