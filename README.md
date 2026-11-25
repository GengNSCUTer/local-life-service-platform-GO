# Local Life Service Platform Go

这是本地生活服务平台的 Go 重构项目。Java 版提供业务和 API 参考，Go 版从模块化单体起步。提供用户、店铺、优惠券、秒杀订单和探店社交五个模块。API 和异步 Worker 可以独立运行。

业务范围、架构图和源码学习路线见[项目总览](docs/15-project-overview.md)。10 万用户、1 万店铺和 100 万博客的造数与查询实测见[目录与社交容量基线](docs/16-catalog-capacity.md)。 百万写链路、常驻连接和收尾回归见[第一版验收](docs/17-v1-validation.md)。

## 运行开发环境

准备 Go 1.27.1 或更新版本和可访问 Docker daemon 的 Docker Compose。从仓库根目录启动本地环境和 API。

```bash
cp .env.example .env
set -a
. ./.env
set +a
docker compose up -d --wait
# 应用数据库迁移后，在两个终端分别执行
go run ./cmd/api
# 另一个终端
go run ./cmd/worker
```

Compose 启动本地 MySQL、Redis 主节点、Redis 副本、三个 Redis Sentinel 和单节点 Kafka。默认只绑定本机，主节点端口为 `26379`，Sentinel 端口为 `26380`、`26381`、`26382`，其他端口分别为 `23306` 和 `29092`。MySQL 初始化 `hmdp` 空库。API 从进程环境变量读取配置，API 与 Compose 可以共用 `.env.example` 中的设置。连接独立服务时，可覆盖 `MYSQL_ADDR`、`REDIS_ADDR`、`REDIS_SENTINEL_ADDRS` 和逗号分隔的 `KAFKA_BROKERS`。`REDIS_PASSWORD` 用于 Redis 数据节点。连接启用了客户端认证的外部 Sentinel 时，另设 `REDIS_SENTINEL_PASSWORD`。

按[秒杀联调步骤](docs/03-seckill-design.md#本地体验)应用 SQL 迁移并准备活动。[用户登录与鉴权](docs/07-user-authentication.md)提供验证码登录、跨实例会话与真实 Token 抢券的联调步骤。

店铺分类、列表、详情与维护的迁移和联调见[店铺与缓存](docs/08-shop-and-cache.md)。

创建普通券和秒杀活动、库存预热重试与订单历史见[优惠券、活动和订单查询](docs/09-voucher-and-order.md)。升级前应用迁移 `016`。

订单取消、pending 超时、补偿重试和异常订单处理见[秒杀订单生命周期](docs/10-order-lifecycle.md)。新库按顺序执行所有迁移。已有库按顺序补齐缺少的迁移。新活动可按配置启用[Redis 活动分片和 MySQL 库存桶](docs/18-seckill-sharding.md)。

百万和千万请求的复现命令与测量口径见[秒杀接口压测](docs/04-seckill-benchmark.md)。

博客、点赞、关注动态、附近店铺和签到见[阶段 5 学习与联调](docs/11-social-and-discovery.md)。原版页面可按[前端点赞适配说明](docs/11-social-and-discovery.md#原版前端的点赞按钮)打包运行。

店铺 UV、博客评论和图片上传见[操作和源码入口](docs/19-uv-comments-media.md)。评论使用迁移 `011`。

[社交通知](docs/20-social-notification-fanout.md)在发帖时按粉丝数自动选择推送或拉取，默认阈值为 1 万。使用迁移 `012` 保存投递模式，`017` 提供分片、失败追踪和通知恢复。

多级回复、爆帖分桶计数、详情缓存、榜单快照与持久化通知任务见[社交高并发说明与对照](docs/22-social-hot-workloads.md)。本轮使用迁移 `014` 和 `015`。

秒杀链路追踪、共享限流、店铺回源熔断和库存补偿命令见[治理和故障演练](docs/21-seckill-governance.md)。手动补偿审计使用迁移 `013`。

[阶段 6 秒杀监控与对账](docs/12-seckill-operations.md)记录 Prometheus、pprof、库存审计、Kafka 高可用配置和故障演练结果。

[阶段 7 API 与 Worker 拆分](docs/13-split-api-worker.md)包含独立启动、部署回滚、备份恢复和持续压测。单进程联调可以运行 `go run ./cmd/all`。

[阶段 8 容量与发布](docs/14-capacity-and-release.md)记录容量测量、生产演练方案和上线回滚步骤。

## 验证 API

```bash
curl -i http://127.0.0.1:8080/health/live
curl -i http://127.0.0.1:8080/health/ready
go test ./...
go vet ./...
```

- `/health/live` 在 HTTP 进程可响应时返回 `200`，可先单独运行 API 来验证启动链路。
- API 的 `/health/ready` 检查 MySQL 和 Redis。Worker 的 `/health/ready` 默认在 `8081` 提供，并检查 Kafka 和后台任务启动状态。失败返回 `503`，`data.checks` 标出对应组件。
- `Ctrl+C` 或 `SIGTERM` 会让 API 停止接收新连接，给正在处理的请求最多 10 秒完成，然后关闭客户端。
- `docker compose down` 停止本地中间件并保留数据卷。

本项目通过了 `go test -race ./...` 和 `go vet ./...`。早期阶段使用独立的 MySQL 8.0.33、Redis 7.2.11 和 Kafka 4.1.2 验证了 Kafka 停机恢复、Redis Sentinel 主从切换、10,000 条 Kafka 长积压恢复、死信自动补偿、非法消息审计和优雅停机。以下 Docker 演练使用独立项目，退出时删除该次演练创建的容器和数据卷。当前环境没有 Docker socket 权限，可使用独立进程运行本机回归。

```bash
scripts/redis-sentinel-smoke.sh
BACKLOG_MESSAGES=10000 scripts/kafka-backlog-smoke.sh
scripts/seckill-compensation-smoke.sh
scripts/order-lifecycle-smoke.sh
```

## 从哪里读代码

- [`cmd/api/main.go`](cmd/api/main.go) 与 [`cmd/worker/main.go`](cmd/worker/main.go) 分别启动 HTTP 和后台任务。
- [`internal/app`](internal/app) 显式组装 GORM、go-redis、kafka-go 和 HTTP 服务。后续在这里注册各业务模块。
- [`internal/user`](internal/user) 实现验证码、用户注册、会话和共用的鉴权中间件。
- [`internal/platform/httpx`](internal/platform/httpx) 管理 JSON 响应、请求 ID、访问日志和 panic 恢复。
- [`docs/`](docs) 记录业务地图、模块架构和秒杀设计。
