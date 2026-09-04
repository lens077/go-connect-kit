package log

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

// ElasticsearchOptions controls request and response body logging.
type ElasticsearchOptions struct {
	RequestBody  bool
	ResponseBody bool
}

// ElasticsearchLogger adapts zap to elastic-transport's Logger interface.
type ElasticsearchLogger struct {
	logger  *zap.Logger
	options ElasticsearchOptions
}

// NewElasticsearchLogger constructs an elastic-transport logger without exposing
// any service-owned protobuf configuration type.
func NewElasticsearchLogger(logger *zap.Logger, options ElasticsearchOptions) *ElasticsearchLogger {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ElasticsearchLogger{logger: logger, options: options}
}

// RequestBodyEnabled reports whether elastic-transport should capture request bodies.
func (logger *ElasticsearchLogger) RequestBodyEnabled() bool {
	return logger.options.RequestBody
}

// ResponseBodyEnabled reports whether elastic-transport should capture response bodies.
func (logger *ElasticsearchLogger) ResponseBodyEnabled() bool {
	return logger.options.ResponseBody
}

// LogRoundTrip records one Elasticsearch request and tolerates connection
// failures where elastic-transport has no HTTP response.
func (logger *ElasticsearchLogger) LogRoundTrip(
	request *http.Request,
	response *http.Response,
	err error,
	_ time.Time,
	duration time.Duration,
) error {
	fields := []zap.Field{zap.Duration("duration", duration)}
	if request != nil {
		fields = append(fields,
			zap.String("method", request.Method),
			zap.String("url", request.URL.String()),
		)
	}
	if response != nil {
		fields = append(fields, zap.Int("status", response.StatusCode))
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
		logger.logger.Warn("elasticsearch request failed", fields...)
		return nil
	}
	logger.logger.Info("elasticsearch request", fields...)
	return nil
}
