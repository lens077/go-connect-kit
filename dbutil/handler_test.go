package dbutil

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq/pqerror"
)

func TestHandleErrorUsesCallerMappingAndLogger(t *testing.T) {
	businessErr := errors.New("duplicate order")
	pgErr := &pgconn.PgError{
		Code:    string(pqerror.UniqueViolation),
		Message: "duplicate key",
		Detail:  "order number already exists",
	}
	wrapped := fmt.Errorf("insert order: %w", pgErr)

	var loggedErr error
	var loggedPgErr *pgconn.PgError
	handler := NewHandler(
		WithErrorMapping(pgErr.Code, businessErr),
		WithLogging(true),
		WithLogger(func(err error, got *pgconn.PgError) {
			loggedErr = err
			loggedPgErr = got
		}),
	)

	got, handled := handler.HandleError(wrapped)
	if !handled {
		t.Fatal("HandleError() handled = false, want true")
	}
	if !errors.Is(got, businessErr) {
		t.Fatalf("HandleError() error = %v, want %v", got, businessErr)
	}
	if loggedErr != wrapped {
		t.Fatalf("logger error = %v, want wrapped input", loggedErr)
	}
	if loggedPgErr != pgErr {
		t.Fatalf("logger PostgreSQL error = %p, want %p", loggedPgErr, pgErr)
	}
}

func TestHandleErrorNoRowsPrecedence(t *testing.T) {
	overrideErr := errors.New("request-specific not found")
	handlerErr := errors.New("handler not found")
	fixedErr := errors.New("fixed not found")

	handler := NewHandler(
		WithNoRowsError(fixedErr),
		WithNoRowsHandler(func(error) error { return handlerErr }),
	)

	got, handled := handler.HandleError(pgx.ErrNoRows, overrideErr)
	if !handled || !errors.Is(got, overrideErr) {
		t.Fatalf("HandleError() = (%v, %t), want override error", got, handled)
	}

	got, handled = handler.HandleError(pgx.ErrNoRows)
	if !handled || !errors.Is(got, handlerErr) {
		t.Fatalf("HandleError() = (%v, %t), want handler error", got, handled)
	}

	got, handled = NewHandler(WithNoRowsError(fixedErr)).HandleError(pgx.ErrNoRows)
	if !handled || !errors.Is(got, fixedErr) {
		t.Fatalf("HandleError() = (%v, %t), want fixed error", got, handled)
	}

	got, handled = NewHandler().HandleError(pgx.ErrNoRows)
	if handled || !errors.Is(got, pgx.ErrNoRows) {
		t.Fatalf("HandleError() = (%v, %t), want original unhandled error", got, handled)
	}
}

func TestMustHandleErrorFormatsPostgresErrors(t *testing.T) {
	tests := []struct {
		name    string
		pgErr   *pgconn.PgError
		wantErr string
	}{
		{
			name:    "unique violation",
			pgErr:   &pgconn.PgError{Code: string(pqerror.UniqueViolation), Detail: "sku already exists"},
			wantErr: "唯一约束冲突: sku already exists",
		},
		{
			name:    "invalid schema",
			pgErr:   &pgconn.PgError{Code: string(pqerror.InvalidSchemaName), SchemaName: "missing_schema"},
			wantErr: "无效的 schema 名称: missing_schema",
		},
		{
			name:    "invalid catalog",
			pgErr:   &pgconn.PgError{Code: string(pqerror.InvalidCatalogName), SchemaName: "missing_catalog"},
			wantErr: "无效的 catalog 名称: missing_catalog",
		},
		{
			name:    "unknown code",
			pgErr:   &pgconn.PgError{Code: "ZZZZZ", Message: "unknown failure"},
			wantErr: "数据库错误 [ZZZZZ]: unknown failure",
		},
	}

	handler := NewHandler()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handler.MustHandleError(tt.pgErr); got == nil || got.Error() != tt.wantErr {
				t.Fatalf("MustHandleError() = %v, want %q", got, tt.wantErr)
			}
		})
	}
}

func TestWrapError(t *testing.T) {
	businessErr := errors.New("conflict")
	pgErr := &pgconn.PgError{Code: string(pqerror.UniqueViolation)}
	handler := NewHandler(WithErrorMapping(pgErr.Code, businessErr))

	if got := handler.WrapError(pgErr, "create product"); !errors.Is(got, businessErr) {
		t.Fatalf("WrapError(mapped) = %v, want business error", got)
	}

	cause := errors.New("connection closed")
	got := handler.WrapError(cause, "create product")
	if !errors.Is(got, cause) {
		t.Fatalf("WrapError(unmapped) = %v, want wrapped cause", got)
	}
	if !strings.Contains(got.Error(), "create product") {
		t.Fatalf("WrapError(unmapped) = %q, want context", got)
	}
}
