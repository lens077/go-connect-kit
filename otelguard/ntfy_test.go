package otelguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNtfyAlerterSendsNotification(t *testing.T) {
	var got *http.Request
	var body string
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		got = request
		payload, _ := readAll(request)
		body = payload
		writer.WriteHeader(http.StatusOK)
		close(done)
	}))
	t.Cleanup(server.Close)

	alerter := NewNtfyAlerter(NtfyConfig{
		URL:   server.URL + "/",
		Topic: "/otel-guard",
		Token: "s3cret",
	}, zap.NewNop())

	alerter.Send(context.Background(), Notification{
		Title:    "指标导出熔断：order",
		Priority: "urgent",
		Tags:     []string{"rotating_light", "warning"},
		Message:  "已熔断",
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the ntfy request")
	}

	assert.Equal(t, "/otel-guard", got.URL.Path)
	assert.Equal(t, "指标导出熔断：order", got.Header.Get("Title"))
	assert.Equal(t, "urgent", got.Header.Get("Priority"))
	assert.Equal(t, "rotating_light,warning", got.Header.Get("Tags"))
	assert.Equal(t, "Bearer s3cret", got.Header.Get("Authorization"))
	assert.Equal(t, "已熔断", body)
}

func TestNtfyAlerterWithoutEndpointIsSilent(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	alerter := NewNtfyAlerter(NtfyConfig{}, zap.New(core))

	alerter.Send(context.Background(), Notification{Title: "x"})
	alerter.Send(context.Background(), Notification{Title: "x"})
	assert.Zero(t, logs.Len())
}

func TestNtfyAlerterWarnsOnRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(server.Close)

	core, logs := observer.New(zapcore.WarnLevel)
	alerter := NewNtfyAlerter(NtfyConfig{URL: server.URL, Topic: "t"}, zap.New(core))

	alerter.Send(context.Background(), Notification{Title: "x", Message: "y"})

	require.Equal(t, 1, logs.Len())
	assert.Contains(t, logs.All()[0].Message, "ntfy notification rejected")
	assert.NotContains(t, logs.All()[0].Message, "s3cret")
}
