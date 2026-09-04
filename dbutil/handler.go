// Package dbutil maps pgx and PostgreSQL errors to caller-owned errors.
package dbutil

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq/pqerror"
)

// Handler configures PostgreSQL error mapping and optional logging.
type Handler struct {
	ErrorMappings map[string]error
	NoRowsError   error
	NoRowsHandler func(err error) error
	EnableLogging bool
	Logger        func(err error, pgErr *pgconn.PgError)
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithErrorMapping maps a PostgreSQL SQLSTATE code to a caller-owned error.
func WithErrorMapping(code string, bizErr error) HandlerOption {
	return func(h *Handler) {
		if h.ErrorMappings == nil {
			h.ErrorMappings = make(map[string]error)
		}
		h.ErrorMappings[code] = bizErr
	}
}

// WithLogging enables or disables the configured logging callback.
func WithLogging(enable bool) HandlerOption {
	return func(h *Handler) {
		h.EnableLogging = enable
	}
}

// WithLogger configures the callback used when logging is enabled.
func WithLogger(logger func(err error, pgErr *pgconn.PgError)) HandlerOption {
	return func(h *Handler) {
		h.Logger = logger
	}
}

// WithNoRowsError maps pgx.ErrNoRows to a fixed caller-owned error.
func WithNoRowsError(bizErr error) HandlerOption {
	return func(h *Handler) {
		h.NoRowsError = bizErr
	}
}

// WithNoRowsHandler maps pgx.ErrNoRows through a callback.
func WithNoRowsHandler(handler func(err error) error) HandlerOption {
	return func(h *Handler) {
		h.NoRowsHandler = handler
	}
}

// NewHandler returns a Handler configured with opts.
func NewHandler(opts ...HandlerOption) *Handler {
	h := &Handler{
		ErrorMappings: make(map[string]error),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// HandleError maps known errors and reports whether a mapping was applied.
func (h *Handler) HandleError(err error, noRowsErr ...error) (error, bool) {
	if err == nil {
		return nil, false
	}

	if errors.Is(err, pgx.ErrNoRows) {
		if h.EnableLogging && h.Logger != nil {
			h.Logger(err, nil)
		}
		if len(noRowsErr) > 0 && noRowsErr[0] != nil {
			return noRowsErr[0], true
		}
		if h.NoRowsHandler != nil {
			return h.NoRowsHandler(err), true
		}
		if h.NoRowsError != nil {
			return h.NoRowsError, true
		}
		return err, false
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		if h.EnableLogging && h.Logger != nil {
			h.Logger(err, nil)
		}
		return err, false
	}

	if h.EnableLogging && h.Logger != nil {
		h.Logger(err, pgErr)
	}

	code := pgErr.Code

	if bizErr, ok := h.ErrorMappings[code]; ok {
		return bizErr, true
	}

	return err, false
}

// WrapError returns a mapped error or wraps an unmapped error with context.
func (h *Handler) WrapError(err error, wrapMsg string) error {
	wrappedErr, handled := h.HandleError(err)
	if handled {
		return wrappedErr
	}
	return fmt.Errorf("%s: %w", wrapMsg, err)
}

// MustHandleError maps known errors and formats all remaining PostgreSQL errors.
func (h *Handler) MustHandleError(err error, noRowsErr ...error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		if h.EnableLogging && h.Logger != nil {
			h.Logger(err, nil)
		}
		if len(noRowsErr) > 0 && noRowsErr[0] != nil {
			return noRowsErr[0]
		}
		if h.NoRowsHandler != nil {
			return h.NoRowsHandler(err)
		}
		if h.NoRowsError != nil {
			return h.NoRowsError
		}
		return fmt.Errorf("not found")
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		if h.EnableLogging && h.Logger != nil {
			h.Logger(err, nil)
		}
		return err
	}

	if h.EnableLogging && h.Logger != nil {
		h.Logger(err, pgErr)
	}

	code := pgErr.Code

	if bizErr, ok := h.ErrorMappings[code]; ok {
		return bizErr
	}

	switch pqerror.Code(code) {
	case pqerror.UniqueViolation:
		return fmt.Errorf("唯一约束冲突: %s", pgErr.Detail)
	case pqerror.ForeignKeyViolation:
		return fmt.Errorf("外键约束冲突: %s", pgErr.Detail)
	case pqerror.NotNullViolation:
		return fmt.Errorf("非空约束冲突: 列 %s 不能为空", pgErr.ColumnName)
	case pqerror.CheckViolation:
		return fmt.Errorf("检查约束冲突: %s", pgErr.ConstraintName)
	case pqerror.RestrictViolation:
		return fmt.Errorf("限制约束冲突: %s", pgErr.ConstraintName)
	case pqerror.ExclusionViolation:
		return fmt.Errorf("排他约束冲突: %s", pgErr.ConstraintName)
	case pqerror.IntegrityConstraintViolation:
		return fmt.Errorf("完整性约束冲突: %s", pgErr.Detail)
	case pqerror.TRDeadlockDetected:
		return fmt.Errorf("检测到TR死锁，请重试")
	case pqerror.LockNotAvailable:
		return fmt.Errorf("锁不可用，请重试")
	case pqerror.StatementTooComplex:
		return fmt.Errorf("语句太复杂，超过程序限制")
	case pqerror.TooManyConnections:
		return fmt.Errorf("连接数过多，请稍后重试")
	case pqerror.DiskFull:
		return fmt.Errorf("磁盘空间不足")
	case pqerror.OutOfMemory:
		return fmt.Errorf("内存不足")
	case pqerror.QueryCanceled:
		return fmt.Errorf("查询被取消")
	case pqerror.TransactionRollback:
		return fmt.Errorf("事务回滚: %s", pgErr.Message)
	case pqerror.InvalidTransactionState:
		return fmt.Errorf("无效的事务状态: %s", pgErr.Message)
	case pqerror.ReadOnlySQLTransaction:
		return fmt.Errorf("只读事务中不能执行写操作")
	case pqerror.FeatureNotSupported:
		return fmt.Errorf("功能不支持: %s", pgErr.Message)
	case pqerror.SyntaxErrorOrAccessRuleViolation:
		return fmt.Errorf("语法错误或访问规则冲突: %s", pgErr.Message)
	case pqerror.UndefinedTable:
		return fmt.Errorf("表不存在: %s", pgErr.TableName)
	case pqerror.UndefinedColumn:
		return fmt.Errorf("列不存在: %s", pgErr.ColumnName)
	case pqerror.UndefinedFunction:
		return fmt.Errorf("函数不存在: %s", pgErr.Message)
	case pqerror.DuplicateTable:
		return fmt.Errorf("表已存在: %s", pgErr.TableName)
	case pqerror.DuplicateColumn:
		return fmt.Errorf("列已存在: %s", pgErr.ColumnName)
	case pqerror.DuplicateFunction:
		return fmt.Errorf("函数已存在: %s", pgErr.Message)
	case pqerror.DuplicateObject:
		return fmt.Errorf("对象已存在: %s", pgErr.Message)
	case pqerror.InvalidCursorState:
		return fmt.Errorf("无效的游标状态: %s", pgErr.Message)
	case pqerror.InvalidSchemaName:
		return fmt.Errorf("无效的 schema 名称: %s", pgErr.SchemaName)
	case pqerror.InvalidCatalogName:
		return fmt.Errorf("无效的 catalog 名称: %s", pgErr.SchemaName)
	case pqerror.InvalidTextRepresentation:
		return fmt.Errorf("无效的文本表示: %s", pgErr.Message)
	default:
		return fmt.Errorf("数据库错误 [%s]: %s", code, pgErr.Message)
	}
}
