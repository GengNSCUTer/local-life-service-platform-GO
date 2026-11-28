# 本机 Docker 验收

2026 年 11 月 28 日在 3090 同一台主机上验证。Docker Engine 28.1.1，Compose 2.35.1，Go 1.27.1，MySQL 8.4.10，Redis 7.2.17，Kafka 4.1.2。测试使用独立回环端口、新建项目和数据卷。API、Worker 使用仓库 Dockerfile 分别构建，容器以 UID `65532` 运行。直接访问 Docker Hub 和默认 Go 模块代理超时，演练用镜像源取得 Redis、Kafka 和构建镜像，用可访问的 Go 代理下载模块。MinIO 使用已有的 `RELEASE.2024-12-18T13-15-44Z` 镜像，Compose 所写的 `RELEASE.2025-04-22T22-12-26Z` 尚待实测。

## 部署与业务回归

| 场景 | 结果 |
| --- | --- |
| 应用镜像与空库 | 应用迁移 `001` 至 `017`，API 和 Worker 健康检查通过。真实验证码登录后抢券并确认，分别重启 API 和 Worker 后仍可查单，取消后 MySQL、Redis 库存均回到 10。 |
| 旧库升级 | 使用迁移 `001` 至 `015` 建库。`016` 为未来开始的活动补建 `pending` 任务，跳过已经开售的活动。`017` 保留未完成任务的游标 512 和完成去重记录，清理过期记录。 |
| Redis Sentinel | 停止主节点，副本提升，切换前后两笔订单确认，新主库存为 0。 |
| 死信补偿 | 制造已有领取资格的冲突，Redis 库存从 1 恢复到 2，本次候选买家占用和预占事件清除，MySQL 库存保持 1。 |
| 订单生命周期 | 确认后取消和重购、Kafka 停机恢复、重放幂等、超时取消、失败后重试回补通过。最终两笔确认、三笔取消，两侧剩余库存均为 3。 |
| MinIO 与通知 | 上传 PNG、匿名下载并校验原字节通过，停对象存储时上传返回 `503`，恢复后重新上传成功。独立通知 Redis 停机期间仍能从 MySQL 读通知，恢复后读请求重建缓存。 |
| Prometheus 与 OpenTelemetry | Prometheus 3.8.1 抓到两个进程的指标，`HMDPDiagnosticsDown` 在停 Worker 后触发、启动后恢复。Jaeger 1.76.0 记录同一条 Trace 中的 HTTP、Redis、Outbox、Kafka Consumer 和 MySQL 五段父子 Span。OTLP 接收端停止期间订单仍确认。 |

## 故障恢复

| 场景 | 结果 |
| --- | --- |
| 三 Broker Kafka | 订单主题副本数 3，`min.insync.replicas=2`。停止分区 0 的 Leader，停止命令返回后约 3 秒内确认后续订单。停止第二个 Broker 后，新订单保持 `pending` 并留在 Redis Outbox。两节点恢复后所有分区 ISR 回到 3，三笔测试订单确认，lag 为 0，剩余库存均为 7。 |
| 三 Broker 冷备恢复 | 停写后导出 MySQL，正常关闭 Redis 和三个 Broker，再备份 Redis AOF 和各 Broker 的 KRaft 数据卷。恢复到新项目后，原确认订单可查，消费组位点和分区末尾位点与备份前相同。启动 Worker 后，Redis 和 Kafka 中各一笔待处理订单都确认，库存归零，ISR 恢复为 3。备份完成后开始恢复到完成这些检查约 36 秒。 |
| Redis Cluster 切主 | 同机三主三从，24 个活动跨槽。停止槽 6220 的主节点，副本约 6 秒接管。恢复探测中 10 次提交返回 `503`，之后一次受理并确认，停主到确认约 30 秒。确认后取消和回补通过。 |
| Redis Cluster 迁槽 | 迁移槽 6220 时发送 40 笔，30 笔受理且全部确认，10 笔返回 `503`。迁槽结束后 24 个活动库存对账一致。故障和迁槽均观察到了写入不可用窗口。 |

## 61 分钟真实下单

使用独立容器部署一套 API、Worker、MySQL、Redis 主从和三 Broker Kafka。一个活动有 36,600 份库存，为 36,600 个合成测试会话分别提交一笔，目标每秒 10 笔。订单主题 3 个分区、3 个副本，Worker 使用 4 个 Relay 和 3 个 Consumer。Redis AOF 使用 `always`。

- 在 3,660.018 秒内受理 36,600 笔，HTTP P99 14.16 ms，零提交失败。停流后 MySQL 最终确认 36,600 笔，领取资格 36,600 条，Redis 与 MySQL 剩余库存均为 0。Redis Outbox、超时索引、未同步补偿与 Kafka lag 清零。
- 定时采样未观察到积压，采样间隔约一分钟。MySQL 记录 14,552 次 InnoDB 行锁等待，累计 62,403 ms。API 和 Worker 的容器内存采样峰值分别为 24.14 MiB 和 32.02 MiB。这个负载档位用于验证持续运行和最终落单，HTTP P99 对应预占受理时延。

## Kafka 独立积压

先把测试订单事件直接写入 Kafka，校验各分区末尾位点之和，再启动消费者。该场景测的是消费和数据库写入。HTTP 受理、Redis 预占与库存一致性使用独立的端到端测试验证。消费侧从启动进程到看到全部分区 lag 为 0，再核对 MySQL 确认数、剩余库存和死信数。

| 消息数 | 活动数 | 分区数 | 排空时间 | 确认数 |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 1 | 3 | 57 秒 | 10,000 |
| 100,000 | 100 | 24 | 83 秒 | 100,000 |
| 1,000,000 | 100 | 24 | 1,355 秒 | 1,000,000 |

百万档在 61 分钟持续下单的环境退出后单独运行。单 Broker，24 个 Consumer，MySQL 最大连接数 48，`innodb_flush_log_at_trx_commit=1`、`sync_binlog=1`。按整个排空窗口计算约 738 笔确认/秒，最终 24 个分区 lag 为 0，MySQL 剩余库存和死信均为 0。

## 复现入口

使用有 Docker 权限的会话和 Go 1.27.1。已加入 `docker` 组的旧会话可通过 `sg docker -c 'bash scripts/redis-sentinel-smoke.sh'` 启动单次演练。共享主机先确认端口空闲，并给每次测试分配新项目和测试端口。`SMOKE_COMPOSE_OVERRIDE` 可传入测试镜像与日志限制配置，构建镜像可用 `--build-arg GO_BUILDER=...`、`--build-arg GOPROXY=...`。

```bash
scripts/redis-sentinel-smoke.sh
scripts/seckill-compensation-smoke.sh
scripts/order-lifecycle-smoke.sh
BACKLOG_MESSAGES=10000 scripts/kafka-backlog-smoke.sh
BACKLOG_MESSAGES=100000 BACKLOG_ACTIVITIES=100 BACKLOG_PARTITIONS=24 \
  BACKLOG_TIMEOUT_SECONDS=900 MYSQL_MAX_OPEN_CONNS=48 scripts/kafka-backlog-smoke.sh
BACKLOG_MESSAGES=1000000 BACKLOG_ACTIVITIES=100 BACKLOG_PARTITIONS=24 \
  BACKLOG_TIMEOUT_SECONDS=2100 MYSQL_MAX_OPEN_CONNS=48 scripts/kafka-backlog-smoke.sh
```

三节点 Kafka 与拆分部署见[阶段 6 运维](12-seckill-operations.md#三节点-kafka-配置)和[阶段 7 演练](13-split-api-worker.md)。容器备份时，可在源容器正常停止后用 `docker cp` 导出所有 Kafka 数据卷和 Redis AOF，在目标项目启动前按服务分别放回对应目录。恢复时保留各 Kafka 节点 ID 和 KRaft cluster ID，使用相同中间件版本，先核对位点再启动 Worker。

持续下单可复用 [固定速率压测脚本](../scripts/seckill-soak.py)。在独立 Compose 环境中预置 `login:token:compose-1` 至 `login:token:compose-36600`，每个 Redis Hash 的 `id` 设为对应的唯一测试用户 ID。用 `prepare-seckill` 准备库存 36,600 的活动 9521，开售后执行 `python3 scripts/seckill-soak.py --port 31260 --voucher 9521 --prefix compose- --seconds 3660 --rate 10`。将端口替换为测试 API 的端口。结束时采集 Worker 的 `/metrics`，并用 `audit-seckill -voucher-id 9521` 核对库存和确认数。

同机容器故障、迁槽和冷备恢复与跨主机实验是独立验收场景。跨主机断网、掉电与 MySQL 主库切换仍待在独立主机上演练。

本轮退出时核对并回收了演练的容器、数据卷、网络、测试应用镜像、临时数据、备份和原始日志。保留可复用的依赖镜像缓存。
