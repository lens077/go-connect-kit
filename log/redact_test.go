package log

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

const (
	sampleBearer = "Bearer abc123.token-xyz"
	sampleJWT    = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	samplePhone  = "13812345678"
	sampleEmail  = "alice@example.com"
)

func redactingLogger(t *testing.T, opts RedactOptions) (*zap.Logger, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	return zap.New(newRedactCore(core, opts)), logs
}

func TestRedactSensitiveFieldKeys(t *testing.T) {
	logger, logs := redactingLogger(t, RedactOptions{})
	logger.Info("sensitive fields",
		zap.String("token", "abc"),
		zap.String("User-Token", "abc"),
		zap.String("password", "abc"),
		zap.String("client_secret", "abc"),
		zap.String("Authorization", "abc"),
		zap.String("api_key", "abc"),
		zap.String("apiKey", "abc"),
		zap.String("phone", samplePhone),
		zap.String("email", sampleEmail),
		zap.String("id_card", "110101199001011234"),
	)

	require.Equal(t, 1, logs.Len())
	fields := logs.AllUntimed()[0].Context
	require.Len(t, fields, 10)
	for i, field := range fields {
		assert.Equal(t, zapcore.StringType, field.Type, "field %d", i)
		assert.Equal(t, DefaultRedactMask, field.String, "field %d key=%s", i, field.Key)
	}
}

func TestRedactLeavesNonSensitiveFieldsUntouched(t *testing.T) {
	logger, logs := redactingLogger(t, RedactOptions{})
	logger.Info("plain fields", zap.String("key", "value"), zap.Int("count", 42))

	require.Equal(t, 1, logs.Len())
	fields := logs.AllUntimed()[0].Context
	require.Len(t, fields, 2)
	assert.Equal(t, "value", fields[0].String)
	assert.Equal(t, int64(42), fields[1].Integer)
}

func TestRedactFreeTextPatternsInMessageAndFields(t *testing.T) {
	logger, logs := redactingLogger(t, RedactOptions{})
	message := "auth " + sampleBearer + " jwt " + sampleJWT +
		" phone " + samplePhone + " email " + sampleEmail
	logger.Info(message, zap.String("detail", message))

	require.Equal(t, 1, logs.Len())
	entry := logs.AllUntimed()[0]
	for _, text := range []string{entry.Message, entry.Context[0].String} {
		assert.NotContains(t, text, "abc123.token-xyz")
		assert.NotContains(t, text, "eyJhbGciOi")
		assert.NotContains(t, text, samplePhone)
		assert.NotContains(t, text, sampleEmail)
		assert.Contains(t, text, DefaultRedactMask)
	}
}

func TestRedactCustomMaskAndExtraKeyPattern(t *testing.T) {
	logger, logs := redactingLogger(t, RedactOptions{
		Mask:             "***",
		ExtraKeyPatterns: []string{"^internal_"},
	})
	logger.Info("custom", zap.String("internal_note", "hide me"), zap.String("token", "hide me"))

	require.Equal(t, 1, logs.Len())
	for _, field := range logs.AllUntimed()[0].Context {
		assert.Equal(t, "***", field.String)
	}
}

func TestNewLoggerRedactionIsOnByDefaultAndCanBeDisabled(t *testing.T) {
	on := captureStdout(t, func() {
		NewLogger(options("info", FormatJSON), testAppInfo).
			Info("hello", zap.String("token", "secret-value"))
	})
	assert.NotContains(t, on, "secret-value")
	assert.Contains(t, on, DefaultRedactMask)

	offOptions := options("info", FormatJSON)
	disabled := false
	offOptions.RedactEnabled = &disabled
	off := captureStdout(t, func() {
		NewLogger(offOptions, testAppInfo).
			Info("hello", zap.String("token", "secret-value"))
	})
	assert.Contains(t, off, "secret-value")

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(on)), &entry))
	assert.Equal(t, DefaultRedactMask, entry["token"])
}
