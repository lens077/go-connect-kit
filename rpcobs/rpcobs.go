// Package rpcobs 是 connect 服务端的 RPC 可观测性拦截器：按错误码分级打日志、
// 给 span 补属性，并把错误的业务 reason 与抛错点（见 errinfo）写进日志、span 与指标。
//
// 它取代此前在 ecommerce 十个服务里各存一份的 server/logging.go。2026-09-24 修
// 「业务异常分支漏记 err 本体」要改十份副本，这是下沉到 kit 的直接原因。
//
// 与 otelconnect 的分工：otelconnect 负责 trace 与 rpc.server.duration 等标准指标，
// 但它只能用 WithAttributeFilter 删属性、不能加属性，所以 error.reason 维度由本包
// 单独的 rpc.server.errors 计数器承载。
package rpcobs

import (
	"context"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/lens077/go-connect-kit/errinfo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

const instrumentationName = "github.com/lens077/go-connect-kit/rpcobs"

// FieldsFunc 为单次请求追加日志字段，例如 payment 需要的 HTTP 方法与路径。
type FieldsFunc func(ctx context.Context, req connect.AnyRequest) []zap.Field

type options struct {
	domain        string
	meterProvider metric.MeterProvider
	extraFields   FieldsFunc
}

// Option 配置 Interceptor。
type Option func(*options)

// WithDomain 设置返回给客户端的 google.rpc.ErrorInfo.Domain，通常用服务注册名。
// reason 只在 domain 内唯一，前端据 (domain, reason) 判断错误。
func WithDomain(domain string) Option {
	return func(o *options) { o.domain = domain }
}

// WithMeterProvider 指定计量提供方；默认使用 otel 全局提供方（kit otel 模块会设置它）。
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithExtraFields 为每条 RPC 日志追加调用方自定义的字段。
func WithExtraFields(fn FieldsFunc) Option {
	return func(o *options) { o.extraFields = fn }
}

// Interceptor 实现 connect.Interceptor，只处理 unary；流式调用原样透传。
type Interceptor struct {
	logger *zap.Logger
	opts   options
	errors metric.Int64Counter
}

// New 构造拦截器。计数器创建失败不影响请求处理，只丢掉 error.reason 指标并记一条日志。
func New(logger *zap.Logger, opts ...Option) *Interceptor {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.meterProvider == nil {
		o.meterProvider = otel.GetMeterProvider()
	}
	i := &Interceptor{logger: logger.Named("LoggingInterceptor"), opts: o}
	counter, err := o.meterProvider.Meter(instrumentationName).Int64Counter(
		"rpc.server.errors",
		metric.WithDescription("RPC errors by connect code and business reason"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		i.logger.Warn("rpc.server.errors counter disabled", zap.Error(err))
	}
	i.errors = counter
	return i
}

func (i *Interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := time.Now()
		resp, err := next(ctx, req)
		duration := time.Since(start)

		span := trace.SpanFromContext(ctx)
		procedure := req.Spec().Procedure

		// connect 的 Code 常量从 1 开始，没有 CodeOK，所以 connect.CodeOf(nil) 会返回
		// CodeUnknown。照抄它就等于把每一次成功调用都记成 "unknown"，日志和链路里
		// 按 rpc.code 做的看板、告警会全部失真。成功路径显式记 "ok"。
		code := "ok"
		if err != nil {
			code = connect.CodeOf(err).String()
		}

		span.SetAttributes(
			attribute.String("rpc.method", procedure),
			attribute.String("rpc.code", code),
			attribute.Int64("rpc.duration_ms", duration.Milliseconds()),
		)

		fields := []zap.Field{
			zap.String("rpc.procedure", procedure),
			zap.String("rpc.code", code),
			zap.String("span_id", span.SpanContext().SpanID().String()),
			zap.String("trace_id", span.SpanContext().TraceID().String()),
			zap.Int64("duration_ms", duration.Milliseconds()),
		}
		if i.opts.extraFields != nil {
			fields = append(fields, i.opts.extraFields(ctx, req)...)
		}

		if err == nil {
			span.AddEvent("rpc_completed", trace.WithAttributes(
				attribute.Int64("duration_ms", duration.Milliseconds()),
			))
			i.logger.Info("rpc completed", fields...)
			return resp, nil
		}

		reason := errinfo.ReasonOf(err)
		attrs := []attribute.KeyValue{attribute.String("error.reason", reason)}
		fields = append(fields, zap.String("error.reason", reason))
		if origin, ok := errinfo.OriginOf(err); ok {
			attrs = append(attrs, attribute.String("error.origin", origin.String()))
			fields = append(fields,
				zap.String("error.origin", origin.String()),
				zap.String("error.function", origin.Function),
			)
		}
		span.SetAttributes(attrs...)
		// 业务异常也要带 err 本体：只记 rpc.code 时 failed_precondition 分不清是哪个哨兵错误。
		fields = append(fields, zap.Error(err))
		span.RecordError(err)
		i.countError(ctx, procedure, code, reason)
		err = i.attachErrorInfo(err, reason)

		message := trace.WithAttributes(attribute.String("message", err.Error()))
		switch connect.CodeOf(err) {
		case connect.CodeInternal, connect.CodeUnknown, connect.CodeDataLoss:
			span.AddEvent("rpc_system_error", message)
			i.logger.Error("rpc system error", fields...)
		case connect.CodeDeadlineExceeded, connect.CodeUnavailable, connect.CodeAborted:
			span.AddEvent("rpc_infrastructure_warning", message)
			i.logger.Warn("rpc infrastructure warning", fields...)
		case connect.CodeCanceled:
			span.AddEvent("rpc_request_canceled", message)
			i.logger.Debug("rpc request canceled by client", fields...)
		default:
			span.AddEvent("rpc_business_exception", message)
			i.logger.Info("rpc business exception", fields...)
		}
		return resp, err
	}
}

func (i *Interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i *Interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func (i *Interceptor) countError(ctx context.Context, procedure, code, reason string) {
	if i.errors == nil {
		return
	}
	service, method := splitProcedure(procedure)
	// 维度与 otelconnect 的 rpc.server.duration 对齐（rpc.service / rpc.method /
	// rpc.connect_rpc.error_code），看板可以直接拿两者做比值。
	i.errors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("rpc.system", "connect_rpc"),
		attribute.String("rpc.service", service),
		attribute.String("rpc.method", method),
		attribute.String("rpc.connect_rpc.error_code", code),
		attribute.String("error.reason", reason),
	))
}

// attachErrorInfo 把 reason 以 google.rpc.ErrorInfo 返回给客户端，前端可按 reason 精确处理。
// 没有声明 reason、或错误里已经带了 ErrorInfo 时保持原样。
func (i *Interceptor) attachErrorInfo(err error, reason string) error {
	if reason == errinfo.Unspecified {
		return err
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return err
	}
	for _, d := range connectErr.Details() {
		if d.Type() == "google.rpc.ErrorInfo" {
			return err
		}
	}
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: reason, Domain: i.opts.domain})
	if detailErr != nil {
		return err
	}
	connectErr.AddDetail(detail)
	return err
}

func splitProcedure(procedure string) (service, method string) {
	trimmed := strings.TrimPrefix(procedure, "/")
	if idx := strings.LastIndex(trimmed, "/"); idx >= 0 {
		return trimmed[:idx], trimmed[idx+1:]
	}
	return "", trimmed
}
