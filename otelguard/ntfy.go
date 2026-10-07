package otelguard

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Notification is one ntfy message produced by a guard state change.
type Notification struct {
	Title    string
	Priority string
	Tags     []string
	Message  string
}

// NtfyConfig configures the ntfy HTTP endpoint. An empty URL or Topic disables
// delivery: the guard keeps working, it just stays silent.
type NtfyConfig struct {
	// URL is the ntfy base address, e.g. https://ntfy.example.com.
	URL string
	// Topic is the ntfy topic, e.g. otel-guard-dev.
	Topic string
	// Token is an optional bearer token; it is never logged.
	Token string

	AlertPriority    string
	AlertTags        []string
	RecoveryPriority string
	RecoveryTags     []string

	// Timeout bounds a single delivery. Client overrides the HTTP client.
	Timeout time.Duration
	Client  *http.Client
}

// DefaultNtfyConfig returns ntfy delivery defaults.
func DefaultNtfyConfig() NtfyConfig {
	return NtfyConfig{
		AlertPriority:    "urgent",
		AlertTags:        []string{"rotating_light"},
		RecoveryPriority: "default",
		RecoveryTags:     []string{"white_check_mark"},
		Timeout:          DefaultRequestTimeout,
	}
}

func (cfg NtfyConfig) normalized() NtfyConfig {
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultRequestTimeout
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: cfg.Timeout}
	}
	if cfg.AlertPriority == "" {
		cfg.AlertPriority = "urgent"
	}
	if cfg.RecoveryPriority == "" {
		cfg.RecoveryPriority = "default"
	}
	if cfg.AlertTags == nil {
		cfg.AlertTags = []string{"rotating_light"}
	}
	if cfg.RecoveryTags == nil {
		cfg.RecoveryTags = []string{"white_check_mark"}
	}
	return cfg
}

// NtfyAlerter delivers Notifications over ntfy's publish API.
type NtfyAlerter struct {
	cfg    NtfyConfig
	client *http.Client
	logger *zap.Logger
}

// NewNtfyAlerter builds an alerter. A nil logger falls back to a no-op logger.
func NewNtfyAlerter(cfg NtfyConfig, logger *zap.Logger) *NtfyAlerter {
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg = cfg.normalized()
	return &NtfyAlerter{cfg: cfg, client: cfg.Client, logger: logger}
}

// Send publishes one notification. It never panics and never returns an error:
// delivery problems are logged as a single warning, because the caller runs it
// off the export path and must not be delayed or retried.
func (alerter *NtfyAlerter) Send(ctx context.Context, notification Notification) {
	if alerter == nil || alerter.cfg.URL == "" || alerter.cfg.Topic == "" {
		return
	}

	endpoint := strings.TrimRight(alerter.cfg.URL, "/") + "/" + strings.TrimLeft(alerter.cfg.Topic, "/")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(notification.Message))
	if err != nil {
		alerter.logger.Warn("otelguard: cannot build ntfy request", zap.Error(err))
		return
	}
	request.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if notification.Title != "" {
		request.Header.Set("Title", notification.Title)
	}
	if notification.Priority != "" {
		request.Header.Set("Priority", notification.Priority)
	}
	if len(notification.Tags) > 0 {
		request.Header.Set("Tags", strings.Join(notification.Tags, ","))
	}
	if alerter.cfg.Token != "" {
		request.Header.Set("Authorization", "Bearer "+alerter.cfg.Token)
	}

	response, err := alerter.client.Do(request)
	if err != nil {
		alerter.logger.Warn("otelguard: ntfy notification failed", zap.String("endpoint", endpoint), zap.Error(err))
		return
	}
	defer func() { _, _ = io.Copy(io.Discard, response.Body); _ = response.Body.Close() }()

	if response.StatusCode >= http.StatusMultipleChoices {
		alerter.logger.Warn("otelguard: ntfy notification rejected",
			zap.String("endpoint", endpoint), zap.Int("status", response.StatusCode))
		return
	}
	alerter.logger.Debug("otelguard: ntfy notification sent", zap.String("title", notification.Title))
}
