# otelguard

给 OpenTelemetry **指标导出器**加一层保护：失败退避、熔断、熔断期间不再刷错误日志、
熔断与恢复各发一次 ntfy 告警、后端恢复后自动复原。

解决的具体问题：某个微服务的 OTLP 端点配错（401/403）、后端 5xx 或网络不通时，
`metric.PeriodicReader` 每个采集周期都会把同一个导出错误打进业务日志，而没有任何主动告警。

## 行为

```
Closed ──连续失败达 MaxFailures──> Open ──ProbeInterval 到──> HalfOpen
   ↑                               │                            │
   └───────────成功────────────────┴──────────成功──────────────┘
                                   │
                                   └──探测失败──> Open（重置 nextProbe，不重复告警）
```

- **退避**：瞬态失败（5xx/408/429/网络）后按 `InitialBackoff × Multiplier^(n-1)` 递增，
  上限 `MaxBackoff`，带 ±`Jitter` 随机抖动。退避窗口内的导出**直接丢弃**（不睡、不阻塞采集）。
- **熔断**：连续失败达到 `MaxFailures` 进入 Open，之后 `Export` 不再调用底层导出器。
- **日志**：失败期间不逐条打日志（只在 Debug 级）；只在 **状态变化** 时打一条 Info/Error。
  再配合 `NewSilentErrorHandler`，Open 时全局 SDK 错误也完全静默。
- **告警**：熔断时发一次 `urgent`，恢复时发一次 `default`；一次故障只告警一次，
  探测失败重开熔断不会重复告警。发送走 goroutine，不阻塞导出路径。
- **半开探测**：Open 每过 `ProbeInterval` 放行一次导出，成功即回到 Closed 并发恢复通知。
- **硬关闭**（可选）：`HardShutdown=true` 时熔断即调用底层 `Shutdown`，之后不再探测。

`Export` **永远返回 nil**，所以 OTel 默认 ErrorHandler 不会再因为指标导出失败而刷日志；
错误只通过日志与 ntfy 呈现。`ForceFlush` 在 Open/已关闭时直接返回 nil，不再等待一个刚失败的端点。

## 接入方式

### 1. 走 kit 的 otel 管道（推荐）

**kit 默认就给指标管道装上保护**：`otel.MetricOptions.Guard` 为空时，`SetupOTelSDK`
用 `otelguard.ConfigFromEnv()` 兜底并安装全局静默错误处理器，服务侧不写一行代码：

```go
shutdown, err := otel.SetupOTelSDK(ctx, info, otel.Options{
	Metric: &otel.MetricOptions{Endpoint: metricEndpoint, TLS: tlsOptions},
}, logger)
```

需要覆盖默认值时（阈值、ntfy）才传 `Guard`，传了就以它为准、完全不再读环境：

```go
guardConfig := otelguard.ConfigFromEnv()
guardConfig.MaxFailures = 4
shutdown, err := otel.SetupOTelSDK(ctx, info, otel.Options{
	Metric: &otel.MetricOptions{
		Endpoint: metricEndpoint,
		TLS:      tlsOptions,
		Guard:    &guardConfig,
	},
}, logger)
```

退出保护用 `Guard: &otelguard.Config{Disabled: true}` 或 `OTEL_GUARD_DISABLED=true`。
**升级注意**：默认开启意味着只升级 kit 版本就会改变导出失败的行为（不再每个采集周期
打一条错误日志，改为退避 + 熔断 + 一条告警），跨仓库升级时按变更说明发布。

### 2. 直接包装导出器

```go
guarded := otelguard.New(exp, cfg, otelguard.WithLogger(logger))
reader := sdkmetric.NewPeriodicReader(guarded, sdkmetric.WithInterval(30*time.Second))
provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
otel.SetErrorHandler(otelguard.NewSilentErrorHandler(guarded.IsOpen, logger))
```

可运行示例：`go run ./otelguard/example`（默认打 `127.0.0.1:4318`，会看到熔断后日志不再刷错误）。

## 配置

```go
type Config struct {
	Disabled         bool          // 透传模式：不计数、不告警、不吞错误
	Service          string        // 只用于日志与告警文案，kit 会自动填服务名
	Endpoint         string        // 同上，kit 会自动填 OTLP 端点
	MaxFailures      int           // 8
	InitialBackoff   time.Duration // 2s
	MaxBackoff       time.Duration // 5m
	Multiplier       float64       // 2.0
	Jitter           float64       // 0.3（0 表示不抖动）
	ProbeInterval    time.Duration // 5m
	HardShutdown     bool          // false
	ShutdownTimeout  time.Duration // 10s
	ErrorLogInterval time.Duration // 30s，SilentErrorHandler 的限流窗口
	Ntfy             NtfyConfig
}

type NtfyConfig struct {
	URL, Topic, Token                        string   // URL/Topic 为空则静默；Token 不落日志
	AlertPriority, RecoveryPriority          string   // urgent / default
	AlertTags, RecoveryTags                  []string // rotating_light / white_check_mark
	Timeout                                  time.Duration // 5s
	Client                                   *http.Client // 可选，默认带 Timeout 的 client
}
```

用 `otelguard.DefaultConfig()` 取上表默认值；零值 `Config{}` 会被补成同样的默认值
（`Jitter` 例外：0 就是 0，避免测试不可复现）。

`ConfigFromEnv()` 在默认值之上读取环境变量，包本身不会主动读环境：

| 环境变量 | 覆盖 |
|---|---|
| `OTEL_GUARD_DISABLED` | `Disabled` |
| `OTEL_GUARD_SERVICE` / `OTEL_GUARD_ENDPOINT` | 告警文案 |
| `OTEL_GUARD_MAX_FAILURES` | `MaxFailures` |
| `OTEL_GUARD_INITIAL_BACKOFF` / `OTEL_GUARD_MAX_BACKOFF` | 退避 |
| `OTEL_GUARD_MULTIPLIER` / `OTEL_GUARD_JITTER` | 退避 |
| `OTEL_GUARD_PROBE_INTERVAL` | 探测间隔 |
| `OTEL_GUARD_HARD_SHUTDOWN` / `OTEL_GUARD_SHUTDOWN_TIMEOUT` | 硬关闭 |
| `OTEL_GUARD_ERROR_LOG_INTERVAL` | 错误日志限流窗口 |
| `OTEL_GUARD_NTFY_URL` / `OTEL_GUARD_NTFY_TOPIC` / `OTEL_GUARD_NTFY_TOKEN` | ntfy |
| `OTEL_GUARD_NTFY_ALERT_PRIORITY` / `OTEL_GUARD_NTFY_RECOVERY_PRIORITY` | ntfy |

Token 是凭据：按仓库约定只放 Config Center 与本地环境（K8s Secret 挂载），不进仓库。

## 错误分类

`Classify(err)` 把错误分成 `permanent`（400/401/403/404，含 gRPC 的
`InvalidArgument`/`Unauthenticated`/`PermissionDenied`/`NotFound`）、`transient`
（5xx/408/429、超时、网络故障）与 `unknown`。分类**不影响阈值**——永久错误同样需要
`MaxFailures` 次观测才熔断——它决定告警文案与排查建议：端点配错时告警里会直接写
「检查 OTLP 端点、Token 与路径」。

OTLP/HTTP 的错误码只出现在错误字符串里（导出器类型未导出），所以分类器除了
`errors.As`/gRPC status 之外还会解析 `403 Forbidden` 这类状态行；导出器自身已重试过的
`retry-able request failure` 与常见网络错误短语按瞬态处理。

## 告警样例

```
Title:    指标导出熔断：order
Priority: urgent
Tags:     rotating_light
Body:     order 的 OTLP 指标导出连续失败 8 次，已熔断。
          错误类型：permanent
          排查建议：鉴权或配置错误，检查 OTLP 端点、Token 与路径
          端点：otlp-dev.apikv.com:443
          最近错误：failed to send metrics to https://otlp-dev.apikv.com:443/v1/metrics: 403 Forbidden (body: (empty))
          恢复探测间隔：5m0s（探测成功后自动恢复）
```

## 测试

`go test -race ./otelguard/...` 覆盖：阈值以下静默、熔断只告警一次且不再调用底层导出器、
退避窗口丢弃导出、半开探测恢复与恢复通知、探测失败重开不重复告警、Open 时错误日志静默、
`ForceFlush` 短路、`Shutdown` 幂等且不再导出、`HardShutdown`、错误分类表、ntfy 请求头与静默。
测试用可注入的假时钟驱动退避，不靠 sleep。

## 与其他包的关系

- 与 `otel` 包的分工：`otel` 负责建管道，本包只包导出器；`otel` 不会在 `Guard` 为空时改变现有行为。
- 只依赖标准库、OpenTelemetry SDK、Zap 与 ntfy 的 HTTP POST。日志用 Zap 而不是 `log/slog`：
  本仓所有包都通过应用 Zap logger 输出，另起一套日志会重新造成两份日志配置。
