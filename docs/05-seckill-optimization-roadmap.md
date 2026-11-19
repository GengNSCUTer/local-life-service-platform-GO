# 秒杀模块后续优化路线

当前秒杀模块已经完成第一阶段闭环。请求通过 Redis Lua 预占库存，事件进入 Redis outbox，Relay 投递 Kafka，Consumer 使用 MySQL 事务落单，异常消息进入 DLQ，对账器根据 MySQL 事实执行审计或自动补偿。

当前基线已经通过以下验证：

- Redis 主从和 Sentinel 故障切换后，Go Redis FailoverClient 可以重新发现新主节点。
- Kafka 长积压 10,000 条时，3 个分区可以恢复消费并清空 lag。
- 冲突订单可以执行幂等库存补偿。
- 非法消息进入 DLQ 后不会阻塞后续消息，并会记录人工复核结果。
- 1,000,000 次和 10,000,000 次 HTTP 请求压测没有系统错误。

当前压测结果属于单机开发环境结果。它用于观察代码行为和发现瓶颈，不能直接作为生产容量承诺。

## 优化原则

后续优化遵循以下顺序：

1. 先保证业务事实和数据一致性。
2. 再补齐监控、告警和故障定位能力。
3. 然后扩大 Redis、Kafka 和 Worker 的横向能力。
4. 最后根据真实指标决定是否拆分服务和增加分片。

秒杀模块在其他业务模块开发期间保持稳定基线。新增业务模块必须继续通过秒杀回归测试，避免基础设施调整破坏已经验证的链路。

## 七个优化点

### 1. 补齐监控指标和故障告警

当前已经有结构化日志、健康检查和压测脚本，但还没有完整的 Prometheus 指标和统一告警。

后续需要增加：

- HTTP 请求总数、状态码分布和 P50、P95、P99、P99.9 延迟。
- Redis Lua 执行次数、错误次数、售罄次数和重复提交次数。
- Redis outbox 当前数量、最早待投递时间和租约超时次数。
- Kafka Producer 成功、失败、重试和发送耗时。
- Kafka Consumer lag、消费耗时和重试次数。
- DLQ 数量、人工复核数量、自动补偿数量和补偿失败数量。
- MySQL 事务成功、回滚、库存不足和唯一键冲突次数。
- Worker 当前 goroutine 数量和优雅停机耗时。

验收标准：

- 可以通过指标判断请求变慢发生在 Redis、Kafka、MySQL 还是应用本身。
- 可以看到每个 Kafka 分区的 lag。
- DLQ 和人工复核数量能够触发告警。
- 日志中的 request_id、order_id、voucher_id、partition 和 offset 可以关联同一条链路。

优先级：P0。

### 2. 增加库存对账和异常处理任务

当前可以处理确定的订单冲突，但库存不一致、Redis 数据丢失、事件长期 pending 等情况会进入人工复核。

后续增加定时对账任务，比较：

- MySQL 活动库存。
- Redis 活动库存。
- MySQL 已确认订单数量。
- Redis 买家占用数量。
- Redis pending 订单数量。
- Redis outbox 数量。
- Kafka 已确认消费位置和 DLQ 数量。

对账结果需要形成明确状态：

- `consistent`：数据一致。
- `repairable`：可以根据事实自动修复。
- `manual_review`：事实不足，需要人工处理。
- `blocked`：依赖不可用，等待下一轮重试。

验收标准：

- 对账任务可以发现 Redis 和 MySQL 库存差异。
- 自动修复必须有幂等标记和审计记录。
- 无法确认事实时不会直接增加库存。
- 对账任务可以重复运行，不产生重复订单或重复补偿。

优先级：P0。

### 3. 扩展 Kafka 高可用和消费能力

当前 Compose 使用单节点 Kafka，适合本地开发。生产环境需要至少三个 Broker，并使用多副本主题。

后续需要验证：

- Kafka Broker 数量为 3。
- 订单主题副本数为 3。
- `min.insync.replicas` 设置为 2。
- Producer 使用 `acks=all`。
- Broker 重启时订单事件不丢失。
- Consumer 重启后从已提交 offset 继续消费。
- Consumer 数量和订单主题分区数匹配。
- Kafka 长时间积压时 lag 可以持续下降。

验收标准：

- 停止一个 Broker 后仍可以提交和消费订单。
- 重启 Consumer 不会产生重复订单。
- Kafka lag、重试和 DLQ 都能被监控。
- 生产配置和本地配置通过不同环境变量管理。

优先级：P1。

### 4. 评估 Redis Cluster 和热点分片

当前 Redis 使用主从加 Sentinel，可以解决主节点故障切换，但所有活动仍然位于一个 Redis 主节点上。

需要先通过指标确认单节点是否成为瓶颈，再决定是否引入 Redis Cluster。

评估内容包括：

- 活动库存 key 的分布策略。
- Redis Cluster hash tag 对 Lua 多 key 操作的影响。
- outbox key 和活动 key 是否需要保持在同一个 slot。
- Redis Cluster 故障迁移期间 Lua 脚本的行为。
- 热门活动集中到单个 slot 时的热点问题。
- Redis Cluster 和 Sentinel 在运维、故障恢复和数据迁移方面的差异。

验收标准：

- Lua 脚本使用的多 key 可以稳定落在同一个 slot。
- 集群扩容和迁移期间秒杀请求行为明确。
- 热门活动不会长期压垮单个节点。
- 发生主节点故障时，客户端可以自动重试并恢复连接。

优先级：P1。

### 5. 拆分 API 和 Seckill Worker

当前 API、Relay、订单 Consumer 和 DLQ Reconciler 在同一个 Go 进程内运行。

后续可以拆成：

```text
API Service
  └── 接收请求、鉴权、Redis Lua 预占库存

Seckill Worker
  ├── Redis outbox Relay
  ├── Kafka Order Consumer
  └── DLQ Reconciler
```

拆分前需要先完成：

- Worker 独立启动入口。
- Worker 独立健康检查。
- Worker 独立配置和日志。
- Worker 独立优雅停机。
- API 与 Worker 之间通过 Redis、Kafka 和 MySQL 协作。
- API 和 Worker 可以单独扩容。

验收标准：

- API 重启不会丢失 Redis outbox 中的事件。
- Worker 重启不会丢失 Kafka 未提交消息。
- API 和 Worker 可以使用不同的副本数量部署。
- Worker 故障不会阻止秒杀请求进入 Redis。

优先级：P1。

### 6. 增加分布式压测和长期稳定性测试

当前百万级和千万级测试主要验证 HTTP 请求总量和客户端 worker 并发。后续需要增加更接近生产环境的测试。

测试范围包括：

- 多台压测机共同发起请求。
- 百万级连接数和连接建立速率。
- 1 小时以上持续压测。
- Kafka 100 万和 1,000 万条事件积压。
- 抢购期间 Redis 主从切换。
- 抢购期间 Kafka Broker 重启。
- MySQL 连接池耗尽和锁等待。
- API、Worker、Redis、Kafka 和 MySQL 的 CPU、内存和网络指标。
- P99.9 延迟和错误分类。

验收标准：

- 明确系统在 Redis、Kafka、MySQL 和 API 层面的瓶颈位置。
- 故障期间没有库存超卖。
- 故障恢复后 pending 订单可以继续处理。
- 长时间运行不会出现 goroutine 泄漏、连接泄漏或 outbox 永久积压。

优先级：P1。

### 7. 完善数据恢复、运维和安全边界

秒杀系统不仅需要代码高并发，还需要可恢复、可发布和可运维。

后续需要补齐：

- MySQL、Redis 和 Kafka 的备份与恢复演练。
- Redis AOF 恢复后 outbox 的一致性检查。
- 数据库 Migration 版本管理和回滚策略。
- 敏感配置使用环境变量或密钥管理系统。
- 管理员操作和库存修复操作的审计日志。
- DLQ 重放工具，支持按订单、时间和原因筛选。
- 订单状态机和异常状态迁移规则。
- 灰度发布和回滚流程。
- 限流、过载保护和活动级开关。

验收标准：

- 可以在隔离环境恢复核心数据。
- 库存修复和 DLQ 重放都有操作记录。
- 管理操作不会绕过业务约束直接修改库存。
- 发布失败时可以回滚应用和数据库变更。

优先级：P1 到 P2。

## 执行顺序

当前阶段先完成其他业务模块。秒杀模块保持现有接口、数据约束和故障回归测试。

其他模块形成稳定业务边界后，按照以下顺序继续优化秒杀：

```text
监控指标
  -> 库存对账
  -> Kafka 多 Broker
  -> 分布式故障演练
  -> Redis Cluster 评估
  -> API 与 Worker 拆分
  -> 长期压测和容量评估
```

每次优化都需要同步更新测试结果、运行参数和已知限制。
