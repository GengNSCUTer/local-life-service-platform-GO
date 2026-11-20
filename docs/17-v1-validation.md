# 第一版验收

第一版覆盖用户、店铺、优惠券、秒杀订单和社交。部署入口、业务链路与源码阅读顺序见[项目总览](15-project-overview.md)。本页记录 3090 上两项规模测试及收尾修复。

## 百万真实订单

测试从 HTTP `POST /voucher-order/seckill/:id` 提交抢券，走鉴权、Redis Lua 预占、outbox Relay、Kafka Consumer 和 MySQL 事务。以 10,000 个已落库的测试账号各买 100 个不同活动构造 100 万个唯一的 `(voucher_id, user_id)` 组合。每个活动 10,000 份库存，压测客户端轮流访问各活动。回环独立环境运行单 Broker 和 24 个订单分区，配置 56 个 Relay、24 个 Consumer，预占期限设为两小时。提交目标为 800 请求/秒。

测试实例的 MySQL `innodb_flush_log_at_trx_commit=2`、`sync_binlog=0` 和 Redis AOF `everysec` 用于测交易链路的处理能力。突然断电后的持久性，以及共享单活动库存行的极限，需分别从故障与单活动演练评估。跨主机恢复的布置见[阶段 8 容量与发布](14-capacity-and-release.md)。

```bash
export MYSQL_BIN_DIR=/path/to/mysql/bin
export REDIS_BIN_DIR=/path/to/redis/bin
export KAFKA_BIN_DIR=/path/to/kafka/bin
STAGE7_BASE_PORT=31220 GOMAXPROCS=12 scripts/stage7-local.sh orders
```

脚本检查客户端 HTTP 202 总数、MySQL 确认记录和领取资格数、全部活动库存、Kafka lag、Redis outbox 与超时索引，以及死信、异常和无账号订单。报告和审计 JSON 保留在 `runtime/stage7-orders.*`，脚本先停止隔离进程，再清理中间件数据目录。

## 百万同时在线连接

同一个私有 Linux 网络命名空间内运行 8 个 Go API 进程。Go 客户端从 64 个 loopback 源地址拨号，复用 HTTP/1.1 连接请求 `/health/live`。每条连接在建连时、保活前、保活后均发出真实 HTTP 请求。服务端连接数取自 `ss` 的 `ESTABLISHED` 采样。脚本记录 API RSS 和宿主机剩余内存，退出时回收自己启动的进程。环境需要允许 `unshare` 创建用户与网络命名空间，并能为压测客户端提供足够的文件描述符。

```bash
CONNECTION_TEST_PROCS=4 GOGC=180 scripts/million-connections.sh
```

2026 年 11 月 20 日复测通过。242.25 秒建成 100 万条连接，逐条探测后从第 250.03 秒开始保活。在 120 秒保活窗口结束后，原连接全部再次返回 HTTP 200，探测于第 377.80 秒完成。服务端连续 7 次采样为 1,000,000 条 `ESTABLISHED`，间隔约 20 秒。8 个 API 的 RSS 合计约 21.1 GiB。首轮曾在升阶过程中出现连接复位。复测关闭压测客户端的探针式 TCP keepalive，并以 `GOGC=180`、每进程 `GOMAXPROCS=4` 运行，未单独定位首轮复位原因。

API 使用 `HTTP_IDLE_TIMEOUT=30m` 允许升阶和保活，应用默认仍是 60 秒。场景验证同机 HTTP 连接维持与通信，负载均衡入口、TLS、远端网络和并发业务请求压力应独立计量。结果保留于 `runtime/million-connections.*`。

## 收尾修复与回归

- 修复 Redis 只淘汰 GEO 索引、还保留就绪标记时短时间误报没有店铺的问题。标记区分空分类和已构建的非空索引，后者需要检查索引确实存在，缺失时通过现有租约重建。覆盖淘汰及空分类的真实 Redis 回归。[GEO 实现](../internal/shop/geo.go)
- 修复 CLI 首次预热已开售的活动时留下 MySQL 活动行的问题。拒绝新增过期时间窗，允许同一 ID 换成未来时间窗后重试。已有活动保留参数校验和重复初始化行为。覆盖失败、重试和幂等的真实 MySQL、Redis 回归。[活动初始化](../internal/seckill/store.go)

运行 `go vet ./...`、`go test -race ./...`，并用真实依赖复测验证码只能核销一次、缓存回填、订单确认和超时并发、重放幂等、库存回补与死信审计。
