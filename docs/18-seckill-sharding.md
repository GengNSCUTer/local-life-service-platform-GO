# 秒杀 Redis 分片与 MySQL 库存桶

## 多个活动并行抢券

开启分片后，新活动按 `voucherID % 16 + 1` 分到 16 个逻辑分片之一。MySQL 保存活动分片，预热、抢券、确认、取消和对账沿用这个分片。以 `seckill:{orders:v2:03}:` 为例，活动库存、买家、预占、备份、补偿标记和两条队列都使用这个前缀。同一笔预占可以在一次 Lua 调用里检查一人一单、扣库存并写队列。不同分片的槽可由不同 Redis Cluster 主节点承载。

新版订单 ID 为 `V2-03-` 加 26 字符随机串。订单尚未写入 MySQL 时，查单和取消也能用 ID 找到预占。HTTP 接口继续返回字符串 `orderId`，客户端应原样保存。Worker 从 MySQL 刷新已使用的分片，轮流领取各片的 Outbox 和超时任务。旧活动继续使用 `{orders}` 和原订单 ID。

`SECKILL_OUTBOX_LIMIT` 和 `SECKILL_PENDING_LIMIT` 分别作用于每条分片队列。原监控仍给出总积压和最慢分片的投递滞后，可用 `hmdp_seckill_shard_queue_size`、`hmdp_seckill_shard_outbox_due_delay_seconds` 和 `hmdp_seckill_shard_queue_sample_up` 定位单片负载与采样失败。

## 单个活动更快落单

启用库存桶后，活动创建事务会把总库存均摊到 `seckill_stock_buckets`。每笔订单在同一个 MySQL 事务内写订单、写领取资格、扣一份桶库存。先试 `userID % 桶数`，首选桶耗尽时再有界地查找其他桶。订单记录实际扣减的桶，取消时原桶归还。领取资格仍以 `(voucher_id, user_id)` 唯一，Kafka 重放也沿用已有订单检查。券列表和对账从桶合计库存，对账同时核对每桶扣减与该桶的确认订单数。

Redis 侧仍按活动保留总库存和一套买家去重数据。MySQL 分桶解决确认事务的活动行竞争。极热单活动的 Redis 受理仍由一个槽承担。当前每笔订单独立提交事务，批量写入可以作为后续单独的性能对照。

## 升级与启用

1. 已有 v1 数据库先应用 `migrations/010_seckill_shards.sql`。它为旧活动保留默认布局 `0`。新库按 `001` 至 `010` 执行。
2. 先更新所有 API 与 Worker，保持新活动的两个功能开关使用默认值。所有进程完成更新后，再为新活动启用 `SECKILL_REDIS_SHARDED=true` 或 `SECKILL_STOCK_BUCKETS=16`。设置 `1` 使用单库存行，库存桶最多 32 个，也受活动库存量限制。
3. 使用 Cluster 时，所有 API、Worker 和活动管理命令配置同一份 `SECKILL_REDIS_CLUSTER_ADDRS`。它使用 `REDIS_PASSWORD`。原 Redis 继续服务会话、其他业务及旧活动。分片键若曾落在原 Redis，切换这些键的后端前需要单独迁移数据。本实现按活动创建时的布局运行，切换配置不会重建或迁移已有活动。
4. 停止创建新版活动可以关闭创建开关。新版活动已创建后，处理这些订单的 API 和 Worker 仍需识别新版布局，并保留对应 Redis 连接。回滚到旧二进制须等新版活动的查询、取消和补偿窗口结束，或先完成数据迁移。

## 3090 实测

2026 年 11 月 23 日，使用隔离的 MySQL 8.0.33、Redis 7.2.11 和 Kafka 4.1.2。MySQL 每次提交刷盘并同步 binlog，Redis AOF 为 `everysec`。每轮使用一个 API、一个 Worker、24 个 Kafka 订单分区。确认吞吐从开始提交算到首次采样发现全量确认且 Redis 队列为空。

| 单券 7,200 笔，每秒提交 400 笔 | 停发后首次采样已确认 | 全量确认用时 | 端到端确认/秒 | MySQL 累计行锁等待 |
| --- | ---: | ---: | ---: | ---: |
| 单库存行 | 136 | 418 秒 | 17.2 | 9,186.6 秒 |
| 16 库存桶 | 2,595 | 56 秒 | 128.6 | 499.8 秒 |

两组的 Redis 逻辑分片都启用，使用相同的单 Redis 节点拓扑。两组订单均全量确认，Kafka 最终 lag 和 Redis 队列均归零，库存与领取资格核对通过。累计行锁等待是所有事务时间之和。单库存行组的部分排空时间与另一个隔离测试共用同机资源，吞吐倍数是这两轮结果，后续同机重复对照应错开其他任务。

| Cluster 场景 | HTTP 受理 | 全量确认用时 | HTTP P99 | Kafka lag 采样峰值 |
| --- | ---: | ---: | ---: | ---: |
| 24 个活动，8 桶，160 请求/秒，75 秒 | 12,000 | 103 秒 | 11 ms | 2,884 |
| 1 个活动，16 桶，80 请求/秒，5 分钟 | 24,000 | 305 秒 | 12 ms | 228 |

24 活动场景的 16 个逻辑分片覆盖三个 Redis Cluster 主节点，验收还包括确认后取消、重复取消、预占后取消和重新抢券。单券持续场景在停止提交的采样点已确认 23,794 笔，随后约 5 秒排空。两个 Cluster 场景均通过逐活动库存对账，零死信和预占异常。MySQL 回归验证了首选桶耗尽后扣其他桶、重放幂等，以及取消后库存回到实际扣减的桶。Cluster 演练使用同机三个主节点，尚待验证带副本的故障切换和迁槽期间的写入行为。

## 本机复现

设置 `MYSQL_BIN_DIR`、`REDIS_BIN_DIR` 和 `KAFKA_BIN_DIR` 后依次执行。脚本退出后会清理自己启动的中间件数据和二进制，并保留采样与对账记录。

```bash
export ORDER_TEST_DURABLE=1
ORDER_TEST_SHARDED=true ORDER_TEST_BUCKETS=1 ORDER_TEST_USERS=7200 ORDER_TEST_ACTIVITIES=1 ORDER_TEST_RATE=400 ORDER_TEST_SAMPLE_SECONDS=3 scripts/stage7-local.sh orders
ORDER_TEST_SHARDED=true ORDER_TEST_BUCKETS=16 ORDER_TEST_USERS=7200 ORDER_TEST_ACTIVITIES=1 ORDER_TEST_RATE=400 ORDER_TEST_SAMPLE_SECONDS=3 scripts/stage7-local.sh orders
ORDER_TEST_CLUSTER=1 ORDER_TEST_BUCKETS=8 ORDER_TEST_USERS=500 ORDER_TEST_ACTIVITIES=24 ORDER_TEST_RATE=160 ORDER_TEST_SAMPLE_SECONDS=3 ORDER_TEST_LIFECYCLE=1 scripts/stage7-local.sh orders
ORDER_TEST_CLUSTER=1 ORDER_TEST_BUCKETS=16 ORDER_TEST_USERS=24000 ORDER_TEST_ACTIVITIES=1 ORDER_TEST_RATE=80 ORDER_TEST_SAMPLE_SECONDS=5 scripts/stage7-local.sh orders
```
