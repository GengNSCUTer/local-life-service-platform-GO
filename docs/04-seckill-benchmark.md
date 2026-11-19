# 秒杀接口压测

压测客户端用 Go 的 `net/http` 请求秒杀接口。测试参数 `-requests` 控制请求总数，`-workers` 控制同时工作的 goroutine 数。单个 goroutine 完成一条 HTTP 请求后才发下一条。每个 goroutine 独立统计结果，结束后通过 channel 汇总，因此统计过程无需为每条请求加一把共享锁。

## 本地复现

先按[秒杀联调步骤](03-seckill-design.md#本地体验)启动测试环境并执行迁移。创建一个库存为 1000 的活动，预留足够的活动时间。下面是在本地 Compose 环境中初始化 16,384 个测试会话的方法。

```bash
for (( i=1; i<=16384; i++ )); do
  printf 'HSET login:token:bench-%s id %s\nEXPIRE login:token:bench-%s 3600\n' "$i" "$i" "$i"
done | docker compose exec -T redis redis-cli --pipe
```

分别为两组请求准备独立的活动。活动开始后依次运行，命令中的 ID 对应各自的活动。

```bash
go run ./cmd/seckill-bench -voucher-id 101 -requests 1000000 -workers 256
go run ./cmd/seckill-bench -voucher-id 102 -requests 10000000 -workers 512
```

客户端输出 JSON，包含吞吐、P50/P95/P99，以及受理、售罄、重复和系统错误数。延迟按毫秒向上分桶。客户端重复使用测试会话并复用 HTTP 连接。活动结束后核对 `seckill_orders` 的确认订单和去重用户数、`seckill_activities` 的剩余库存、Redis 活动库存和待投递队列。

## 测试口径

API、压测客户端、MySQL、Redis 和 Kafka 都在同一台共享开发机上，API 与压测客户端各设置 `GOMAXPROCS=6`。秒杀后台使用默认的 4 个 Relay goroutine 和 3 个 Kafka Consumer Reader。依赖为 MySQL 8.0.33、Redis 7.2.11（AOF 每次写入同步）和 Kafka 4.1.2。每组活动库存为 1000，使用同一批 16,384 个测试用户。结果描述的是包含鉴权、库存判断及售罄和重复响应的 HTTP 请求吞吐。Kafka 投递和 MySQL 写入仅发生在受理成功时，落库结果在测后单独核对。

## 2026 年 11 月 19 日实测

| 总请求数 | 并发客户端 | 耗时 | 请求/秒 | P50 | P95 | P99 | 系统错误 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 256 | 29.50 s | 33,898 | 7 ms | 13 ms | 16 ms | 0 |
| 10,000,000 | 512 | 280.68 s | 35,628 | 14 ms | 20 ms | 23 ms | 0 |

| 总请求数 | 受理 | 售罄 | 重复提交 | 最终确认订单 | 去重用户 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 1,000 | 938,424 | 60,576 | 1,000 | 1,000 |
| 10,000,000 | 1,000 | 9,389,000 | 610,000 | 1,000 | 1,000 |

两组压测结束时，MySQL 和 Redis 的剩余库存均为 0，Redis 待投递队列均为空。

## 故障与恢复验证

三类长时间场景可以直接运行仓库脚本。脚本使用独立的 Compose 项目名和端口，结束时移除容器，适合在本机重复执行。

```bash
scripts/redis-sentinel-smoke.sh
BACKLOG_MESSAGES=10000 scripts/kafka-backlog-smoke.sh
scripts/seckill-compensation-smoke.sh
```

`redis-sentinel-smoke.sh` 先提交一笔订单，再停止 Redis 主节点，等待 Sentinel 将副本提升为主节点，然后通过同一个 Go Redis FailoverClient 提交第二笔订单。脚本检查两个订单都进入 MySQL，并检查新主节点上的 Redis 库存为零。

`kafka-backlog-smoke.sh` 在 API 启动前向订单主题写入一批结构合法的订单事件，启动 API 后轮询 `hmdp-seckill-orders-v1` 的 Kafka lag，直到积压归零，再核对确认订单数和 MySQL 库存。`BACKLOG_MESSAGES` 默认是 10,000，可以按机器性能调整。

`seckill-compensation-smoke.sh` 预先在 MySQL 写入同一买家的已有订单，再通过正常 HTTP 请求制造一条 Redis 预占事件。消费者将它送入死信，对账器核对 MySQL 事实后自动补偿。脚本检查死信审计结果为 `compensated`、Redis 库存恢复、买家占用和订单事件被删除、MySQL 库存保持不变。

自动补偿有明确边界。无效消息、MySQL 库存不一致、Redis 买家映射缺失或映射不属于当前事件时，结果是 `manual_review`，系统不会直接增加库存。仓库脚本使用 Docker Compose 复现完整拓扑，当前开发环境能够通过 Compose 配置检查，但 Docker socket 权限不足，所以 Compose 容器演练尚未在当前环境启动。相同的本机进程级 Sentinel、Kafka 长积压和补偿演练已经通过。
