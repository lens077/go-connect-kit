package log

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/lens077/go-connect-kit/config"
	"github.com/lens077/go-connect-kit/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var testAppInfo = meta.AppInfo{
	ID:          "test-service-id",
	Name:        "test-service",
	Host:        "localhost",
	Environment: "dev",
	Version:     "v0.0.1",
}

func options(level, format string) Options {
	return Options{
		Level:               level,
		Format:              format,
		FrameworkLogLevel:   "info",
		FrameworkErrorLevel: "error",
	}
}

func captureStdout(t *testing.T, run func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = original }()

	done := make(chan string, 1)
	go func() {
		contents, _ := io.ReadAll(reader)
		done <- string(contents)
	}()

	run()
	require.NoError(t, writer.Close())
	output := <-done
	require.NoError(t, reader.Close())
	return output
}

func logAllLevels(t *testing.T, level, format string) string {
	t.Helper()
	return captureStdout(t, func() {
		logger := NewLogger(options(level, format), testAppInfo)
		logger.Debug("msg-debug")
		logger.Info("msg-info")
		logger.Warn("msg-warn")
		logger.Error("msg-error")
	})
}

func TestNewLoggerFiltersLevels(t *testing.T) {
	tests := []struct {
		level   string
		want    []string
		notWant []string
	}{
		{level: "debug", want: []string{"msg-debug", "msg-info", "msg-warn", "msg-error"}},
		{level: "info", want: []string{"msg-info", "msg-warn", "msg-error"}, notWant: []string{"msg-debug"}},
		{level: "warn", want: []string{"msg-warn", "msg-error"}, notWant: []string{"msg-debug", "msg-info"}},
		{level: "error", want: []string{"msg-error"}, notWant: []string{"msg-debug", "msg-info", "msg-warn"}},
	}
	for _, test := range tests {
		t.Run(test.level, func(t *testing.T) {
			output := logAllLevels(t, test.level, FormatJSON)
			for _, value := range test.want {
				assert.Contains(t, output, value)
			}
			for _, value := range test.notWant {
				assert.NotContains(t, output, value)
			}
		})
	}
}

func TestTeeCoresFiltersOTelSinkAndHotReloads(t *testing.T) {
	observedCore, observed := observer.New(zapcore.DebugLevel)
	level := zap.NewAtomicLevelAt(zapcore.WarnLevel)
	logger := zap.New(teeCores(zapcore.NewNopCore(), observedCore, level))

	logger.Debug("before-debug")
	logger.Info("before-info")
	logger.Warn("before-warn")
	require.Equal(t, 1, observed.Len())
	assert.Equal(t, "before-warn", observed.AllUntimed()[0].Message)

	level.SetLevel(zapcore.DebugLevel)
	logger.Debug("after-debug")
	require.Equal(t, 2, observed.Len())
	assert.Equal(t, "after-debug", observed.AllUntimed()[1].Message)
}

func TestNewLoggerInvalidLevelFallsBackToDebug(t *testing.T) {
	assert.Contains(t, logAllLevels(t, "invalid-level", FormatJSON), "msg-debug")
}

func TestNewLoggerEmptyLevelIsInfo(t *testing.T) {
	output := logAllLevels(t, "", FormatJSON)
	assert.NotContains(t, output, "msg-debug")
	assert.Contains(t, output, "msg-info")
}

func TestNewLoggerJSONFormat(t *testing.T) {
	output := captureStdout(t, func() {
		NewLogger(options("info", FormatJSON), testAppInfo).
			Info("hello", zap.String("key", "value"), zap.Int("number", 42))
	})

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(output)), &entry))
	assert.Equal(t, "hello", entry["msg"])
	assert.Equal(t, "value", entry["key"])
	assert.EqualValues(t, 42, entry["number"])
	assert.Contains(t, entry["caller"], "log_test.go:")
}

func TestNewLoggerConsoleFormat(t *testing.T) {
	output := captureStdout(t, func() {
		NewLogger(options("info", FormatConsole), testAppInfo).Info("hello")
	})
	assert.Contains(t, output, "hello")
	assert.NotContains(t, output, `"msg"`)
	assert.Contains(t, output, "INFO")
}

func TestNewLoggerUnknownFormatFallsBackToJSON(t *testing.T) {
	output := captureStdout(t, func() {
		NewLogger(options("info", "invalid-format"), testAppInfo).Info("hello")
	})
	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(output)), &entry))
	assert.Equal(t, "hello", entry["msg"])
}

func logProject(message *wrapperspb.StringValue) Options {
	return options(message.GetValue(), FormatJSON)
}

func TestModuleProvidesLogger(t *testing.T) {
	initial := wrapperspb.String("info")
	live := config.NewLive(initial)
	var logger *zap.Logger
	app := fx.New(
		fx.NopLogger,
		fx.Supply(initial, testAppInfo, live),
		Module(logProject),
		fx.Populate(&logger),
	)
	require.NoError(t, app.Err())
	assert.NotNil(t, logger)
}

func TestFxLoggerAcceptsValidAndInvalidLevels(t *testing.T) {
	for _, level := range []string{"info", "not-a-level", ""} {
		t.Run(level, func(t *testing.T) {
			initial := wrapperspb.String("info")
			live := config.NewLive(initial)
			_ = captureStdout(t, func() {
				app := fx.New(
					fx.Supply(initial, testAppInfo, live),
					Module(logProject),
					FxLogger(func(message *wrapperspb.StringValue) Options {
						result := logProject(message)
						result.FrameworkLogLevel = level
						result.FrameworkErrorLevel = level
						return result
					}),
				)
				assert.NoError(t, app.Err())
			})
		})
	}
}

func TestModuleHotReloadsLogLevel(t *testing.T) {
	initial := wrapperspb.String("warn")
	live := config.NewLive(initial)
	output := captureStdout(t, func() {
		var logger *zap.Logger
		app := fx.New(
			fx.NopLogger,
			fx.Supply(initial, testAppInfo, live),
			Module(logProject),
			fx.Populate(&logger),
		)
		require.NoError(t, app.Err())

		logger.Debug("before-hot-reload")
		live.Set(wrapperspb.String("debug"))
		logger.Debug("after-hot-reload")
	})
	assert.NotContains(t, output, "before-hot-reload")
	assert.Contains(t, output, "after-hot-reload")
	assert.Contains(t, output, "log level changed")
}
