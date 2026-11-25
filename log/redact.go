package log

import (
	"regexp"
	"strings"

	"go.uber.org/zap/zapcore"
)

// DefaultRedactMask replaces the value of a sensitive field or a sensitive
// fragment inside a free-text value.
const DefaultRedactMask = "[REDACTED]"

// RedactOptions configures the redaction core. Its zero value uses the
// built-in rules and DefaultRedactMask.
type RedactOptions struct {
	// Mask replaces sensitive values. Empty means DefaultRedactMask.
	Mask string
	// ExtraKeyPatterns adds field-name patterns (Go regexp, case-insensitive
	// matching is the caller's responsibility) to the built-in sensitive-key
	// list. They are matched against the original, non-normalized key.
	ExtraKeyPatterns []string
}

var (
	// Sensitive keys are matched after lower-casing and removing separators,
	// so token, user_token, user-token and userToken all hit the same rule.
	sensitiveKeyTokens = []string{
		"token", "password", "passwd", "secret", "authorization",
		"apikey", "phone", "email", "idcard", "credential",
		"privatekey", "accesskey", "refreshkey",
	}

	// Free-text patterns. RE2 has no lookaround, so word boundaries are used.
	bearerPattern = regexp.MustCompile(`(?i)Bearer\s+[A-Za-z0-9._~+/=-]+`)
	jwtPattern    = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`)
	phonePattern  = regexp.MustCompile(`\b1[3-9][0-9]{9}\b`)
	emailPattern  = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
)

var separators = strings.NewReplacer("_", "", "-", "", ".", "")

// redactCore wraps a zapcore.Core and scrubs fields before they reach it.
// It must override Check so the *wrapper* (not the wrapped core) is added to
// the CheckedEntry; otherwise zap would call the wrapped core's Write and
// skip redaction entirely.
type redactCore struct {
	zapcore.Core
	mask  string
	extra []*regexp.Regexp
}

func newRedactCore(core zapcore.Core, opts RedactOptions) zapcore.Core {
	mask := opts.Mask
	if mask == "" {
		mask = DefaultRedactMask
	}
	extra := make([]*regexp.Regexp, 0, len(opts.ExtraKeyPatterns))
	for _, pattern := range opts.ExtraKeyPatterns {
		if compiled, err := regexp.Compile(pattern); err == nil {
			extra = append(extra, compiled)
		}
	}
	return redactCore{Core: core, mask: mask, extra: extra}
}

func (core redactCore) Enabled(level zapcore.Level) bool {
	return core.Core.Enabled(level)
}

func (core redactCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if !core.Enabled(entry.Level) {
		return checked
	}
	entry.Message = redactText(entry.Message, core.mask)
	return checked.AddCore(entry, core)
}

func (core redactCore) With(fields []zapcore.Field) zapcore.Core {
	return redactCore{
		Core:  core.Core.With(core.redactFields(fields)),
		mask:  core.mask,
		extra: core.extra,
	}
}

func (core redactCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	entry.Message = redactText(entry.Message, core.mask)
	return core.Core.Write(entry, core.redactFields(fields))
}

func (core redactCore) redactFields(fields []zapcore.Field) []zapcore.Field {
	if len(fields) == 0 {
		return nil
	}
	out := make([]zapcore.Field, len(fields))
	copy(out, fields)
	for i, field := range out {
		if core.sensitiveKey(field.Key) {
			out[i] = zapcore.Field{Key: field.Key, Type: zapcore.StringType, String: core.mask}
			continue
		}
		if field.Type == zapcore.StringType {
			out[i].String = redactText(field.String, core.mask)
		}
	}
	return out
}

func (core redactCore) sensitiveKey(key string) bool {
	normalized := strings.ToLower(separators.Replace(key))
	for _, token := range sensitiveKeyTokens {
		if strings.Contains(normalized, token) {
			return true
		}
	}
	for _, pattern := range core.extra {
		if pattern.MatchString(key) {
			return true
		}
	}
	return false
}

func redactText(text, mask string) string {
	if text == "" {
		return text
	}
	text = bearerPattern.ReplaceAllString(text, "Bearer "+mask)
	text = jwtPattern.ReplaceAllString(text, mask)
	text = emailPattern.ReplaceAllString(text, mask)
	text = phonePattern.ReplaceAllString(text, mask)
	return text
}
