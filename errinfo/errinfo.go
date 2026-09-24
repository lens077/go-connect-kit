// Package errinfo 给错误补上两类排障证据：稳定的业务 reason 与抛错点。
//
// 为什么需要：service 层把领域错误映射成 connect 的 14 个错误码之后，日志和指标里
// 只剩 failed_precondition 这种粗粒度的码，分不清是「购物车为空」还是「库存不足」；
// 而日志由 RPC 拦截器统一打印，zap 的 caller 永远指向拦截器自己，没法据此 git blame。
// （2026-09-24，对照腾讯错误码治理实践：排障要同时拿到错误语义、代码位置、运行时证据。）
//
// 本包只依赖标准库，领域层可以放心导入。
package errinfo

import (
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"strings"
)

// Unspecified 是没有声明 reason 的错误在指标与日志里使用的取值。
const Unspecified = "UNSPECIFIED"

// reasonPattern 限定 reason 为有界的 UPPER_SNAKE_CASE（同 google.rpc.ErrorInfo 的约定）。
// reason 会成为指标 label，放开格式就等于放开基数。
var reasonPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,62}[A-Z0-9]$`)

// Error 是带 reason 的哨兵错误。用指针身份比较，errors.Is 行为与 errors.New 相同。
type Error struct {
	reason string
	msg    string
}

// New 声明一个带 reason 的哨兵错误，用法与 errors.New 相同：
//
//	var ErrCartEmpty = errinfo.New("CART_EMPTY", "[order] cart is empty")
//
// reason 不合法时直接 panic：哨兵都是包级变量，错误会在进程启动时暴露，而不是在线上悄悄变成高基数 label。
func New(reason, msg string) *Error {
	if !reasonPattern.MatchString(reason) {
		panic(fmt.Sprintf("errinfo: reason %q must be UPPER_SNAKE_CASE, 2-64 chars", reason))
	}
	return &Error{reason: reason, msg: msg}
}

func (e *Error) Error() string { return e.msg }

// Reason 返回声明时的 reason。
func (e *Error) Reason() string { return e.reason }

// ReasonOf 沿错误链找到第一个声明了 reason 的错误；找不到返回 Unspecified。
func ReasonOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.reason
	}
	return Unspecified
}

// Origin 是错误被记录下来的源码位置。
type Origin struct {
	File     string
	Line     int
	Function string
}

func (o Origin) String() string {
	return fmt.Sprintf("%s:%d", o.File, o.Line)
}

type originError struct {
	err    error
	origin Origin
}

func (e *originError) Error() string { return e.err.Error() }
func (e *originError) Unwrap() error { return e.err }

// Here 把调用者的源码位置附到 err 上。err 为 nil 时返回 nil。
// 链上已经有更深的位置时保持原样：最靠近故障的那一处最有用。
func Here(err error) error {
	return HereSkip(err, 1)
}

// HereSkip 与 Here 相同，但跳过额外 skip 层调用栈，供 dbutil 这类包装函数记录其调用者。
func HereSkip(err error, skip int) error {
	if err == nil {
		return nil
	}
	var existing *originError
	if errors.As(err, &existing) {
		return err
	}
	pc, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return err
	}
	fn := ""
	if f := runtime.FuncForPC(pc); f != nil {
		fn = f.Name()
	}
	return &originError{err: err, origin: Origin{File: trimPath(file), Line: line, Function: fn}}
}

// OriginOf 返回错误链上记录的源码位置。
func OriginOf(err error) (Origin, bool) {
	var e *originError
	if errors.As(err, &e) {
		return e.origin, true
	}
	return Origin{}, false
}

// trimPath 只保留模块内的相对路径，去掉构建机的绝对前缀：
// 日志里要的是能拿去 git blame 的路径，构建机路径既无用又会泄露环境信息。
func trimPath(file string) string {
	// 不用 "/pkg/" 作标记：它会误匹配模块缓存路径 $GOPATH/pkg/mod/...
	for _, marker := range []string{"/services/", "/internal/"} {
		if i := strings.Index(file, marker); i >= 0 {
			return strings.TrimPrefix(file[i:], "/")
		}
	}
	if i := strings.LastIndex(file, "/"); i >= 0 {
		if j := strings.LastIndex(file[:i], "/"); j >= 0 {
			return file[j+1:]
		}
	}
	return file
}
