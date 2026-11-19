# Local Life Service Platform Go

这是本地生活服务平台的 Go 重构版本。

项目以同级目录中的 Java 项目 `../` 作为业务基线。Java 版本已经覆盖短信登录、商户缓存、优惠券秒杀、Redis Stream 异步下单、关注 Feed、附近商户和签到等场景。Go 版本会保留业务语义和主要 HTTP API，同时结合 Go 的并发模型、显式依赖管理和服务生命周期重新设计实现。

## 当前阶段

当前先完成业务与技术架构梳理，文档位于 `docs/`。下一阶段会初始化 Go 模块、配置本地基础设施，并实现可运行的健康检查服务。

## 技术路线

- HTTP：Gin
- ORM：GORM
- Redis：go-redis
- 消息队列：Kafka，客户端先采用 `segmentio/kafka-go`
- 关系数据库：MySQL
- 本地基础设施：Docker Compose
- 后续观测：Prometheus、Grafana、OpenTelemetry

## 目录

- `docs/`：业务地图、架构设计、组件职责和秒杀设计
- `cmd/`：可执行程序入口，后续创建
- `internal/`：按业务领域组织的应用代码，后续创建
- `migrations/`：数据库迁移和兼容性变更，后续创建
- `deploy/`：本地基础设施配置，后续创建
