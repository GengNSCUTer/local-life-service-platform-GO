# Local Life Service Platform Go

这是本地生活服务平台的 Go 重构项目。Java 版提供业务和 API 参考，Go 版从模块化单体起步。首个业务模块使用 Redis Lua 预占秒杀库存，后台投递 Kafka 订单事件，消费者通过 MySQL 事务落单。

## 运行开发环境

准备 Go 1.27.1 或更新版本和可访问 Docker daemon 的 Docker Compose。从仓库根目录启动本地环境和 API。

```bash
cp .env.example .env
set -a
. ./.env
set +a
docker compose up -d --wait
go run ./cmd/api
```

Compose 启动本地 MySQL、Redis 主节点、Redis 副本、三个 Redis Sentinel 和单节点 Kafka。默认只绑定本机，主节点端口为 `26379`，Sentinel 端口为 `26380`、`26381`、`26382`，其他端口分别为 `23306` 和 `29092`。MySQL 初始化 `hmdp` 空库。API 从进程环境变量读取配置，API 与 Compose 可以共用 `.env.example` 中的设置。连接独立服务时，可覆盖 `MYSQL_ADDR`、`REDIS_ADDR`、`REDIS_SENTINEL_ADDRS` 和逗号分隔的 `KAFKA_BROKERS`。

按[秒杀联调步骤](docs/03-seckill-design.md#本地体验)执行两份 SQL 迁移、准备活动并体验抢券与订单查询。

百万和千万请求的复现命令与测量口径见[秒杀接口压测](docs/04-seckill-benchmark.md)。

## 验证 API

```bash
curl -i http://127.0.0.1:8080/health/live
curl -i http://127.0.0.1:8080/health/ready
go test ./...
go vet ./...
```

- `/health/live` 在 HTTP 进程可响应时返回 `200`，可先单独运行 API 来验证启动链路。
- `/health/ready` 并行检查 MySQL Ping、Redis Ping 和 Kafka Broker 元数据。依赖全部可用时返回 `200`，有依赖不可用时返回 `503`，并在 `data.checks` 中标出对应组件。
- `Ctrl+C` 或 `SIGTERM` 会让 API 停止接收新连接，给正在处理的请求最多 10 秒完成，然后关闭客户端。
- `docker compose down` 停止本地中间件并保留数据卷。

本阶段通过了 `go test -race ./...`、`go vet ./...` 和 Compose 配置检查。使用独立的 MySQL 8.0.33、Redis 7.2.11 和 Kafka 4.1.2 验证了依赖全部就绪、Kafka 停机恢复、Redis Sentinel 主从切换、10,000 条 Kafka 长积压恢复、死信自动补偿、非法消息审计和 API 收到 `SIGTERM` 后退出。仓库提供了三个可复现脚本，分别验证 Redis Sentinel 故障切换、Kafka 长积压恢复和死信自动补偿。当前环境的 Docker daemon 访问权限限制了 Compose 脚本的实际启动验证，但本机进程级故障演练已经通过。

```bash
scripts/redis-sentinel-smoke.sh
BACKLOG_MESSAGES=10000 scripts/kafka-backlog-smoke.sh
scripts/seckill-compensation-smoke.sh
```

## 从哪里读代码

- [`cmd/api/main.go`](cmd/api/main.go) 读取环境配置，监听进程信号并启动应用。
- [`internal/app`](internal/app) 显式组装 GORM、go-redis、kafka-go 和 HTTP 服务。后续在这里注册各业务模块。
- [`internal/platform/httpx`](internal/platform/httpx) 管理 JSON 响应、请求 ID、访问日志和 panic 恢复。
- [`docs/`](docs) 记录业务地图、模块架构和秒杀设计。
