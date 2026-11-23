# 第一版验收

第一版覆盖用户、店铺、优惠券、秒杀订单和社交。部署入口、业务链路与源码阅读顺序见[项目总览](15-project-overview.md)。本页记录 3090 上两项规模测试及收尾修复。

## 百万真实订单

测试从 HTTP `POST /voucher-order/seckill/:id` 提交抢券，走鉴权、Redis Lua 预占、outbox Relay、Kafka Consumer 和 MySQL 事务。以 10,000 个已落库的测试账号各买 100 个不同活动构造 100 万个唯一的 `(voucher_id, user_id)` 组合。每个活动 10,000 份库存，压测客户端轮流访问各活动。回环独立环境运行单 Broker 和 24 个订单分区，配置 56 个 Relay、24 个 Consumer，预占期限设为两小时。提交目标为 800 请求/秒。

2026 年 11 月 20 日实测通过。HTTP 在 20 分 50 秒内受理 100 万笔，P99 为 56 ms，零系统错误。提交结束后的首个采样已确认 596,075 笔，第 52 分 14 秒采样确认达到 **1,000,000** 笔，按整个窗口计算约 319 笔确认/秒。全部 100 个活动对账一致，Redis outbox、超时索引、Kafka lag 和剩余库存均归零。取消、死信、预占异常和无账号订单均为零。Worker 记录了 2,596 次处理重试。

对账完成后，临时 MySQL 超过原 30 秒退出宽限，脚本强制回收该测试进程并清理其数据目录。后续复现给这个大数据模式 120 秒的 MySQL 退出宽限。此次报告使用退出前留存的数据库汇总、队列指标与逐活动对账结果。

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

## 开启 MySQL 提交刷盘后的真实落单复测

2026 年 11 月 23 日在 3090 上逐轮启动独立的 MySQL、Redis、Kafka 和 Go 进程。每轮只有一个 API 和一个 Worker。Worker 开启 56 个 Relay 和 24 个 Consumer，Kafka 单 Broker、24 个订单分区。MySQL 8.0.33 的数据目录在 `/disk2`，启用 binlog，`innodb_flush_log_at_trx_commit=1`、`sync_binlog=1`。Redis AOF 仍为 `everysec`。

两轮 400 请求/秒测试分别把 36,000 笔订单均匀分到 24 个活动，或全部集中在同一个活动。单活动使用 36,000 个账号，多活动使用 1,500 个账号，各账号在每个活动只提交一次。单活动 36,000 笔场景在完成 90 秒入流和首个停发采样后结束，并另用 4,800 笔验证完整落单和对账。10 分钟持续测试按 100 请求/秒覆盖 24 个活动，使用 2,500 个账号，共 60,000 笔。三轮都通过 HTTP 抢券接口，提交通路与百万订单演练相同。

```bash
export MYSQL_BIN_DIR=/path/to/mysql/bin
export REDIS_BIN_DIR=/path/to/redis/bin
export KAFKA_BIN_DIR=/path/to/kafka/bin
export GOMAXPROCS=12 ORDER_TEST_DURABLE=1 ORDER_TEST_SAMPLE_SECONDS=5
STAGE7_BASE_PORT=31440 ORDER_TEST_RATE=400 ORDER_TEST_USERS=1500 ORDER_TEST_ACTIVITIES=24 scripts/stage7-local.sh orders
STAGE7_BASE_PORT=31460 ORDER_TEST_RATE=400 ORDER_TEST_USERS=36000 ORDER_TEST_ACTIVITIES=1 scripts/stage7-local.sh orders
STAGE7_BASE_PORT=31460 ORDER_TEST_RATE=400 ORDER_TEST_USERS=4800 ORDER_TEST_ACTIVITIES=1 scripts/stage7-local.sh orders
STAGE7_BASE_PORT=31480 ORDER_TEST_RATE=100 ORDER_TEST_USERS=2500 ORDER_TEST_ACTIVITIES=24 scripts/stage7-local.sh orders
```

`orders-progress.tsv` 记录确认数、Redis outbox、待处理预占、Kafka lag 和 MySQL 行锁等待。运行至排空的场景检查全部订单及领取资格、逐活动库存、死信、预占异常、Redis 队列，并通过 Kafka CLI 核对全部 24 个订单分区的 lag 归零。行锁等待时间为测试期间所有事务的累计值。应用每 10 秒刷新 Kafka lag，采样峰值可能低于实际峰值。

400 请求/秒的两轮各受理 36,000 笔，售罄、重复请求和 HTTP 错误均为零。停发后首次采样时，多活动约确认 121 单/秒，单活动约确认 20 单/秒。同期单次 MySQL 行锁等待均值从约 148 ms 增至 1,151 ms。代码在订单事务中先更新活动库存行，再写订单和领取资格。订单 ID 将消息分散到多个 Kafka 分区，单活动的事务仍会争用同一条库存行。

| 400 请求/秒场景 | HTTP P99 | 停发后首次采样的确认数 | 同一采样 Kafka lag | 同一采样累计行锁等待 |
| --- | ---: | ---: | ---: | ---: |
| 24 个活动 | 14 ms | 11,110 | 23,822 | 764.7 秒 |
| 1 个活动 | 7 ms | 1,775 | 33,685 | 2,072.1 秒 |

24 个活动的 36,000 笔在第 260 秒全部确认，约比提交结束晚 170 秒，Kafka lag 采样峰值 24,407 条。单活动的大流量场景结束时还有约 34,200 笔预占待处理，用于对比送单期间的压力。两轮首次停发采样分别在第 92 和 91 秒取得。两组是在共享主机上的单轮对比，多活动运行期间还有一次未成功启动的测试 MySQL 短暂初始化数据目录。

单活动 4,800 笔在约 12 秒内全部受理，第 208 秒全部确认且 Redis 队列清零，Kafka 的 24 个订单分区最终 lag 为零。最终 4,800 笔确认均有领取资格，库存对账一致，零取消、死信、预占异常和无账号订单。MySQL 记录 4,798 次行锁等待，累计 4,453.0 秒。

24 个活动以 100 请求/秒持续提交 10 分钟，HTTP 受理 60,000 笔，P99 为 11 ms。第 603 秒的首个停发采样已确认全部订单，Redis outbox 和预占超时索引归零。整个窗口内 Kafka lag 采样峰值 51 条，待处理预占峰值 172 笔，两者没有持续增长。最终 24 个活动对账一致，Kafka 的 24 个订单分区 lag 为零，零取消、死信、预占异常和无账号订单。这个结果覆盖 3090 本机单 Worker 的一档持续负载，容量上限还取决于活动分布和存储配置。

三组完整落单场景共确认 100,800 笔。测试环境的数据目录、二进制和大日志已清理，本机 `runtime/stage7-orders.*` 留有请求汇总、积压曲线和对账记录。
