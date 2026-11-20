# 阶段 8 · 容量评估与发布演练

## 复现本机容量测试

在仓库根目录配置 MySQL、Redis 和 Kafka 的二进制目录。脚本会创建独立数据库、Redis AOF 目录和 Kafka 日志目录，并在退出时停止自己启动的进程。

```bash
export MYSQL_BIN_DIR=/path/to/mysql/bin
export REDIS_BIN_DIR=/path/to/redis/bin
export KAFKA_BIN_DIR=/path/to/kafka/bin
STAGE7_BASE_PORT=30140 CAPACITY_SECONDS=20 CAPACITY_RATES='5 15 30' scripts/stage7-local.sh capacity
```

测试端把请求轮流发给同一台机器上的两个 API 进程。两个 Worker 分别启动 2 个和 1 个 Consumer，共用三个订单分区。每档使用独立活动，向活动提交不同买家的抢券请求。每秒集中发起一批，客户端为每个请求新建连接。Redis 打开 AOF 并每次写入同步，Kafka 使用本机单 Broker，MySQL 使用一个实例。

每档记录 `202` 接收数、HTTP P99、HTTP 结束时的确认数和最终确认数。`capacity.tsv` 的排空时间包含确认订单与后台采样指标归零的等待时间，也包含脚本查询的开销。最终核对 Redis 买家数、MySQL 确认订单数和两边库存。最高档抓取 API 与 Worker 的 CPU 和堆快照，测试目录包含对应二进制。

```bash
go tool pprof -top /path/to/run/api /path/to/run/api.cpu.pprof
go tool pprof -top /path/to/run/worker /path/to/run/worker.cpu.pprof
go tool pprof -top -sample_index=inuse_space /path/to/run/worker /path/to/run/worker.heap.pprof
```

`net/http` 的短连接测试包含建连开销。容量评估时还要用 Keep-Alive 复测请求吞吐，用独立场景测常驻连接。`pprof` 的 CPU Profile 统计运行时间，数据库 I/O 与锁等待结合事务耗时和 MySQL 锁等待指标看。小样本 CPU Profile 只用来确认采集链路和初步定位。

## 本机实测

| 测试请求/秒 | 持续时间 | HTTP 受理 | HTTP P99 | 发完请求时已确认 | 排空与核对窗口 | 最终确认 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 20 秒 | 100 | 50.07 ms | 100 | 0 秒 | 100 |
| 15 | 20 秒 | 300 | 195.29 ms | 300 | 2 秒 | 300 |
| 30 | 20 秒 | 600 | 170.86 ms | 367 | 16 秒 | 600 |

三档合计 1,000 笔各自活动内的真实抢券，两个 API 分别收到了 500 笔。各活动的最终库存均为 0，Redis 两个队列与 Kafka lag 最终清零。最高档在 HTTP 结束时还有 233 笔待确认，需要用更长、持续负载的对照试验评估稳定落单速度。最高档两个 CPU Profile 分别只有 40 ms、300 ms 的有效样本，堆快照均可读取。短档 P99 受单次采样波动影响。

MySQL 进程停止后，API 的 `/health/ready` 返回 `503`，`/health/live` 返回 `200`。直连 API 的抢券请求进入 Redis 预占。用原数据目录重启 MySQL 后，该订单确认，库存、Kafka lag 和 Redis 队列全部归零。阶段 7 还验证了 Redis Sentinel 切主和 Kafka 单 Broker 崩溃。若入口负载均衡器按 `/health/ready` 摘除实例，MySQL 停机时的外部接单表现由入口路由决定。

上一阶段的 61 分钟演练收到了 36,600 笔 `202`，最终确认 34,260 笔，超时取消 2,340 笔。更早的千万级测试统计了 HTTP 请求总量，其中最终确认 1,000 笔。两类历史结果分别见[阶段 7 演练](13-split-api-worker.md)和[HTTP 压测](04-seckill-benchmark.md)。

## 容量模型与扩容依据

| 观测点 | 扩容与复测依据 |
| --- | --- |
| API CPU、HTTP P99 | 对同一档流量比较 1 个与多个 API 实例，记录 HTTP 接收和超时，观察增加实例后 Redis 响应是否也变慢 |
| Redis Lua、待处理预占 | 观测预占耗时、命令延迟和队列大小。待处理上限按落单速度和允许排队时间估算，初始估算为 `确认订单/秒 × 可接受排队秒数`，再用故障与突发流量回测 |
| Kafka 分区 lag | 持续上涨时先看每个分区的流入与消费，再增加 Reader 或分区。当前 3 个分区，配置 Consumer 总数时同时核对 Kafka 分配和 MySQL 吞吐 |
| MySQL 事务、库存锁 | 用 `mysql_persist` 耗时、MySQL 锁等待、连接池等待判断瓶颈。活动库存由同一行承担，增加 Worker 后仍需观察热点行。只读副本可用于合适的历史查询，扣库存和实时状态依赖主库数据 |

估算排空时间时，停止入流后用 `积压量 ÷ 实测消费速度`。入流持续时用 `积压量 ÷ (实测消费速度 − 新订单进入速度)`，两者差值为正时积压才会收敛。多个 Worker 采集的是同一组 Kafka lag 和同一份共享队列，仪表板按分区或队列取最大值来去重。

秒杀 Lua 使用 `{orders}` 标签，把活动、预占和全局 outbox 放在一起。目前这些键集中在一个槽。评估 Redis Cluster 要同时设计按活动或分片的 outbox 和超时索引、Worker 的分片扫描，以及分片级背压。热点活动是否需要进一步切库存桶，要用多活动和单活动对比实验决定。Kafka 扩分区后要观察新旧消息分布，单活动 MySQL 写入仍走同一条库存行。

## 百万连接和多主机验收方案

1. 在独立主机部署负载均衡器、至少两个 API、两个 Worker 和压测端。Kafka 与 Redis 使用跨主机的副本拓扑。每台主机记录 CPU、内存、数据盘、网卡、文件描述符额度和本地源端口范围。
2. 把连接目标按压测机和源 IP 分摊。单个目标地址与端口下，每个源 IP 能使用的连接数受本地源端口范围影响。先测 1 万、10 万，再测 30 万和 100 万常驻连接。每档记录服务端 `ESTABLISHED`、连接成功率、建连速率、请求 P99、进程 RSS 和文件描述符占用。压测端与入口代理也要采样。
3. 到达目标后持续保活，叠加真实抢券。分别测试 HTTP Keep-Alive 连接、售罄和重复请求、不同用户真实落单。记录每类流量的速率、确认结果与积压，明确可接受的 HTTP 与落单延迟。
4. 在持续流量下分别停止一台 API、Kafka Broker 所在主机和 Redis 主节点所在主机。用阶段 7 的订单 ID 核对方式统计故障窗口内的受理、确认、取消和差异。MySQL 主库切换与多 Broker 备份恢复另设场景，记录恢复耗时和可恢复的数据范围。

## 发布检查和回滚

1. 在隔离环境按顺序执行 `001` 至 `009`，记录应用版本、数据库版本、备份位置和恢复结果。发布 API 与 Worker 时使用同一套 Redis key、Kafka topic 与消费组配置。
2. 先准备新 Worker，检查就绪、积压和库存对账，再把部分流量交给新 API。分别验证登录、店铺查询、抢券 `202`、订单最终确认、取消和补偿。逐步加流量并观察 HTTP 错误、队列和数据库锁等待。
3. 如果队列持续增长、未处理补偿增加或对账出现差异，暂停放量。用 `request_id`、`order_id`、Kafka partition 与 offset 关联日志，记录受影响活动和最后一个健康版本。
4. 回滚时先把 API 流量切回上个版本，再通过 `SIGTERM` 退出新 Worker，启动上个兼容版本并检查消费进度。阶段 8 沿用阶段 7 的 `001` 至 `009`，应用回滚可复用存储。恢复备份前先暂停写入，在隔离环境验证快照，明确恢复点，再依据实际故障切换。
5. 回滚后再次核对活动库存、MySQL 状态、Redis 队列、Kafka lag、死信和人工复核记录。开发环境可用 `cmd/all` 做兼容性联调。
