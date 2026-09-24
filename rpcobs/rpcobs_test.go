package rpcobs

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/lens077/go-connect-kit/errinfo"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/emptypb"
)

var errCartEmpty = errinfo.New("CART_EMPTY", "[order] cart is empty")

func invoke(t *testing.T, opts []Option, returned error) (*observer.ObservedLogs, *sdkmetric.ManualReader, error) {
	t.Helper()
	core, logs := observer.New(zap.DebugLevel)
	reader := sdkmetric.NewManualReader()
	opts = append(opts, WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	interceptor := New(zap.New(core), opts...)
	next := func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) { return nil, returned }
	req := connect.NewRequest(&emptypb.Empty{})
	_, err := interceptor.WrapUnary(next)(context.Background(), req)
	return logs, reader, err
}

// 回归：业务异常曾只记 rpc.code、不带 err 本体（2026-09-24，ecommerce 十份副本同病）。
// 下沉后同时要求带上 reason，并以 ErrorInfo 交给客户端。
func TestBusinessExceptionCarriesErrorAndReason(t *testing.T) {
	wrapped := fmt.Errorf("checkout: %w", errCartEmpty)
	logs, reader, err := invoke(t, []Option{WithDomain("order-service")},
		connect.NewError(connect.CodeFailedPrecondition, wrapped))

	if !errors.Is(err, errCartEmpty) {
		t.Fatal("interceptor must pass the original error chain through")
	}
	entries := logs.FilterMessage("rpc business exception").All()
	if len(entries) != 1 {
		t.Fatalf("want 1 business exception log, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got, _ := fields["error"].(string); !strings.Contains(got, errCartEmpty.Error()) {
		t.Fatalf("log must carry the error body, fields=%v", fields)
	}
	if fields["error.reason"] != "CART_EMPTY" {
		t.Fatalf("error.reason = %v, want CART_EMPTY", fields["error.reason"])
	}

	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		t.Fatal("want connect error")
	}
	var info *errdetails.ErrorInfo
	for _, d := range connectErr.Details() {
		v, valueErr := d.Value()
		if valueErr != nil {
			t.Fatal(valueErr)
		}
		if ei, ok := v.(*errdetails.ErrorInfo); ok {
			info = ei
		}
	}
	if info == nil || info.GetReason() != "CART_EMPTY" || info.GetDomain() != "order-service" {
		t.Fatalf("client must receive ErrorInfo{CART_EMPTY, order-service}, got %v", info)
	}

	if got := errorCount(t, reader, "CART_EMPTY", "failed_precondition"); got != 1 {
		t.Fatalf("rpc.server.errors{CART_EMPTY} = %d, want 1", got)
	}
}

// 抛错点必须是记录 Here 的那一行，而不是拦截器：拦截器的行号无法拿去 blame。
func TestSystemErrorCarriesOrigin(t *testing.T) {
	cause, wantLine := errinfo.Here(errors.New("connection reset")), callerLine() // origin 应指向这一行
	logs, reader, _ := invoke(t, nil,
		connect.NewError(connect.CodeUnknown, fmt.Errorf("query order: %w", cause)))

	entries := logs.FilterMessage("rpc system error").All()
	if len(entries) != 1 {
		t.Fatalf("want 1 system error log, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	origin, _ := fields["error.origin"].(string)
	if want := fmt.Sprintf("rpcobs_test.go:%d", wantLine); !strings.HasSuffix(origin, want) {
		t.Fatalf("error.origin = %q, want the Here() call site %s", origin, want)
	}
	if fields["error.reason"] != errinfo.Unspecified {
		t.Fatalf("undeclared reason must fall back to %s, got %v", errinfo.Unspecified, fields["error.reason"])
	}
	if got := errorCount(t, reader, errinfo.Unspecified, "unknown"); got != 1 {
		t.Fatalf("rpc.server.errors{UNSPECIFIED,unknown} = %d, want 1", got)
	}
}

func errorCount(t *testing.T, reader *sdkmetric.ManualReader, reason, code string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "rpc.server.errors" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("rpc.server.errors has unexpected type %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				r, _ := dp.Attributes.Value("error.reason")
				c, _ := dp.Attributes.Value("rpc.connect_rpc.error_code")
				if r.AsString() == reason && c.AsString() == code {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func callerLine() int {
	_, _, line, _ := runtime.Caller(1)
	return line
}
