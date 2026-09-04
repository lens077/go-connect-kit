# go-connect-kit

微服务共享基础设施库。收敛此前在每个服务里各存一份的基础设施模块，消除复制粘贴漂移。

## 为什么存在

服务骨架由 [`go-connect-template-cli`](https://github.com/lens077/go-connect-template-cli)
从 [`go-connect-template`](https://github.com/lens077/go-connect-template) 一次性生成，
生成之后没有回流通道：模板改了，已生成的服务不会跟着变。

后果是同一份基础设施代码在 10 个服务里各存一份并各自漂移。2026-09-01 在 ecommerce 实测：
`services/*/internal/pkg` 有 15,106 行生产 Go 代码，其中 `registry/consul.go` 有 8 个变体、
`log/log.go` 有 4 个变体。

仅在业务仓内上提只能解决存量。模板服务于新项目，它无法 import 某个业务仓的 `pkg`，
所以新生成的服务仍会带回副本。**要让根因收敛，共享实现必须放在一个可被外部导入的模块里**
——也就是本仓。

## 当前内容

| 包 | 职责 | 外部依赖 |
|---|---|---|
| `env` | 环境变量读取与类型转换 | 无（仅标准库） |
| `meta` | 应用身份（`AppInfo`）与构建版本（`Version`） | 无（仅标准库） |
| `config` | 严格解码、Protovalidate 校验、原子替换、订阅与最后已知正常值 | protobuf、Viper、Fx |
| `configschema` | 用生成的 JSON Schema 校验 YAML，并只返回脱敏错误路径 | JSON Schema、YAML |
| `log` | Zap 构造、Fx 事件日志与应用日志级别热更新 | Zap、OTel bridge、`config.Live` |
| `otel` | trace、metric、log 管道，资源、传播、运行时与数据库/Redis 埋点辅助 | OpenTelemetry |
| `registry` | Consul 注册、TTL 心跳、可选 gRPC 就绪检查与故障自恢复 | Consul API、Fx |
| `dbutil` | pgx / PostgreSQL 错误识别、业务错误映射与可选日志回调 | `pgx/v5`、`lib/pq/pqerror` |

这些包只接收提供方无关的 Go `Options`，不导入消费方的 protobuf。消费方保留很薄的
protobuf-to-options adapter；配置源的具体实现也由消费方适配 `config.Source` / `config.Watcher`。
删除本模块后，加载、热更新、日志、遥测、注册恢复和数据库错误映射会重新散回所有调用方，
因此共享实现只在这里维护一份。

BSR 不参与这条发布链路：BSR 分发 proto，不分发 Go 实现。本模块作为普通 Go module 发布。

## 版本约束

`go.mod` 的 go directive 是**天花板**：必须小于等于所有消费方里最低的那个版本。
当前消费方为 ecommerce（go 1.27.0）、control-tower 与模板（均为 go 1.26.5），因此本模块声明 `go 1.26.0`。

抬高它之前先确认所有消费方都已升级——否则低版本那一侧编译不过，且报错发生在别人的仓库里。

## 可见性

本仓必须**公开**。消费方的镜像构建走 `GOPROXY=https://goproxy.cn,direct` 且没有配置
`GOPRIVATE`，Docker 构建阶段也没有挂载任何凭据。改成私有会让所有服务的镜像构建直接失败。

## `meta.Version` 与 ldflags

`meta.Version` 是构建版本的注入目标，默认值 `dev`：

```
-ldflags="-X github.com/lens077/go-connect-kit/meta.Version=$VERSION"
```

**Go linker 对不存在的 `-X` 目标是静默忽略的，不报错。** 改动这个变量的包路径或名字，
必须同步改所有消费方的 Dockerfile，否则版本注入会无声失效——ecommerce 曾因此让 10 份
Dockerfile 的注入长期无效，跑着的二进制无法自报由哪次构建产生。

`meta.Version` 表示**构建制品版本**，与 `AppInfo.Version`（API 契约版本，形如 `v1`，
会进 Consul 服务标签与 OTel `service.version`）是两个不同概念，不要合并。
