// Package rpcobs 是 connect v2 服务端的 RPC 可观测性拦截器：按错误码分级打日志、
// 给 span 补属性，并把错误的业务 reason 与抛错点（见 errinfo）写进日志、span 与指标。
//
// 它取代此前在 ecommerce 十个服务里各存一份的 server/logging.go。2026-09-24 修
// 「业务异常分支漏记 err 本体」要改十份副本，这是下沉到 kit 的直接原因。
//
// kit v1.0.0 起基于 connect-go v2：拦截器是 ServerInterceptor 函数类型，一份实现
// 同时覆盖 unary 与 streaming——v1 只包 WrapUnary、流式调用完全不记日志的盲区就此
// 消失。v2 服务端拦截器在解码之前运行，拿不到已解码的消息，请求元数据（headers、
// 协议、peer）改经 CallInfo 获取。
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

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/lens077/go-connect-kit/errinfo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

const instrumentationName = "github.com/lens077/go-connect-kit/rpcobs"

// FieldsFunc 为单次请求追加日志字段，例如网关转发的调用方标识。
// v2 服务端拦截器在解码之前运行，只有 CallInfo 可看（headers、协议、peer）；
// info 可能是 nil（不经 Server.Call 的裸 ServerFunc 测试），实现须对 nil 安全。
type FieldsFunc func(ctx context.Context, info *connect.CallInfo) []zap.Field

type options struct {
	domain        string
	meterProvider metric.MeterProvider
	extraFields   FieldsFunc
}

// Option 配置拦截器。
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

// New 构造 connect v2 服务端拦截器，同时覆盖 unary 与 streaming。计数器创建失败
// 不影响请求处理，只丢掉 error.reason 指标并记一条日志。
func New(logger *zap.Logger, opts ...Option) connect.ServerInterceptor {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.meterProvider == nil {
		o.meterProvider = otel.GetMeterProvider()
	}
	log := logger.Named("LoggingInterceptor")
	counter, counterErr := o.meterProvider.Meter(instrumentationName).Int64Counter(
		"rpc.server.errors",
		metric.WithDescription("RPC errors by connect code and business reason"),
		metric.WithUnit("{error}"),
	)
	if counterErr != nil {
		log.Warn("rpc.server.errors counter disabled", zap.Error(counterErr))
	}

	return func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			start := time.Now()
			err := next(ctx, spec, stream)
			duration := time.Since(start)

			span := trace.SpanFromContext(ctx)
			procedure := spec.Procedure

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
			if o.extraFields != nil {
				info, _ := connect.CallInfoForServerContext(ctx)
				fields = append(fields, o.extraFields(ctx, info)...)
			}

			if err == nil {
				span.AddEvent("rpc_completed", trace.WithAttributes(
					attribute.Int64("duration_ms", duration.Milliseconds()),
				))
				log.Info("rpc completed", fields...)
				return nil
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
			countError(ctx, counter, procedure, code, reason)
			err = attachErrorInfo(err, reason, o.domain)

			message := trace.WithAttributes(attribute.String("message", err.Error()))
			switch connect.CodeOf(err) {
			case connect.CodeInternal, connect.CodeUnknown, connect.CodeDataLoss:
				span.AddEvent("rpc_system_error", message)
				log.Error("rpc system error", fields...)
			case connect.CodeDeadlineExceeded, connect.CodeUnavailable, connect.CodeAborted:
				span.AddEvent("rpc_infrastructure_warning", message)
				log.Warn("rpc infrastructure warning", fields...)
			case connect.CodeCanceled:
				span.AddEvent("rpc_request_canceled", message)
				log.Debug("rpc request canceled by client", fields...)
			default:
				span.AddEvent("rpc_business_exception", message)
				log.Info("rpc business exception", fields...)
			}
			return err
		}
	}
}

func countError(ctx context.Context, counter metric.Int64Counter, procedure, code, reason string) {
	if counter == nil {
		return
	}
	service, method := splitProcedure(procedure)
	// 维度与 otelconnect 的 rpc.server.duration 对齐（rpc.service / rpc.method /
	// rpc.connect_rpc.error_code），看板可以直接拿两者做比值。
	counter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("rpc.system", "connect_rpc"),
		attribute.String("rpc.service", service),
		attribute.String("rpc.method", method),
		attribute.String("rpc.connect_rpc.error_code", code),
		attribute.String("error.reason", reason),
	))
}

// attachErrorInfo 把 reason 以 google.rpc.ErrorInfo 附给客户端，前端可按 reason 精确处理。
// 没有声明 reason、或错误里已经带了 ErrorInfo 时保持原样。
//
// v2 的 *Error 是值语义：WithDetail 返回副本、不改原链。所以这里重建一个携带同样
// code/message/ErrorInfo 的新错误、把原链整个挂成 cause——errors.Is/As 与日志里的
// err 本体都保持原样，线上多出来的只有 ErrorInfo。
func attachErrorInfo(err error, reason, domain string) error {
	if reason == errinfo.Unspecified {
		return err
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return err
	}
	for _, d := range connectErr.Details() {
		if d.Type == "google.rpc.ErrorInfo" {
			return err
		}
	}
	detail, detailErr := connectproto.NewErrorDetail(&errdetails.ErrorInfo{Reason: reason, Domain: domain})
	if detailErr != nil {
		return err
	}
	rebuilt := connect.NewError(connectErr.Code(), connectErr.Message()).WithDetail(detail)
	if connectErr.IsRemote() {
		rebuilt = rebuilt.WithRemote()
	}
	return rebuilt.WithCause(err)
}

func splitProcedure(procedure string) (service, method string) {
	trimmed := strings.TrimPrefix(procedure, "/")
	if idx := strings.LastIndex(trimmed, "/"); idx >= 0 {
		return trimmed[:idx], trimmed[idx+1:]
	}
	return "", trimmed
}
