# Go 秒杀设计

## 1. 为什么先做秒杀

秒杀同时包含 Redis 原子操作、数据库事务、唯一约束、消息队列、消费者并发、失败重试和最终一致性，能把 Go 语言和后端中间件的关键问题放进一条完整链路中验证。

## 2. 第一版组件边界

```text
SeckillHandler
    -> SeckillService.Accept
        -> RedisQualification
        -> OrderEventProducer

OrderConsumer
    -> OrderProcessor
        -> VoucherOrderRepository.WithTransaction
        -> OffsetCommitter

ReconciliationWorker
    -> OrderRepository
    -> RedisCompensator
    -> AuditProducer
```

每个边界都可以使用接口替身测试。HTTP Handler 不应该知道 Kafka partition 或 MySQL 锁细节。

## 3. Redis 资格脚本

首版保留 Java 项目已经验证过的原子资格判断语义：

```text
输入：stock key、ordered-user key、user id、voucher id、order id

1. stock key 不存在，返回活动未初始化
2. stock <= 0，返回库存不足
3. user 已在 ordered-user set，返回重复下单
4. stock - 1
5. ordered-user set 加入 user
6. 生成订单事件
7. 返回成功
```

Go 版本直接使用 Kafka 生产订单事件。Redis Stream 只作为 Java 基线的对照材料，用来理解 Pending、ACK、重试和死信等问题，不作为 Go 版本的生产链路。

## 4. Kafka 事件

```json
{
  "schema_version": 1,
  "event_id": "stable-event-id",
  "order_id": 100001,
  "user_id": 7,
  "voucher_id": 200,
  "occurred_at": "2026-11-19T10:00:00Z"
}
```

事件 ID 与订单 ID 必须在重试时保持不变。Producer 发送失败时，HTTP 请求不能假装成功。Redis 资格已经预占但 Kafka 写入失败时，需要有明确的补偿入口，后续实现会比较同步补偿、Outbox 和定时扫描三种方案。

## 5. MySQL 事务

消费者拿到事件后执行：

```text
BEGIN
  检查 user_id + voucher_id 是否已经有订单
  条件更新库存 stock = stock - 1 WHERE voucher_id = ? AND stock > 0
  插入订单
COMMIT
```

如果已有同一业务订单，视为幂等成功并提交 offset。如果数据库库存不足，说明 Redis 预占与 MySQL 状态发生漂移，不能简单 ACK，需要进入对账或人工处理路径。

## 6. Go Consumer 生命周期

Consumer 必须由应用统一启动和关闭：

1. 应用启动时检查 Topic、Consumer Group 和数据库连接。
2. 使用 `context.WithCancel` 启动消费 goroutine。
3. 消费循环读取消息并设置单条处理超时。
4. 进程收到 SIGTERM 后停止拉取新消息。
5. 等待当前事务完成并提交已成功消息的 offset。
6. 到达关闭超时后关闭 Kafka、Redis 和 MySQL 连接。

每个 goroutine 都必须响应 Context 取消，不能使用没有退出条件的后台死循环。

## 7. 失败分类

| 类型 | 处理 |
| --- | --- |
| JSON 或 schema 无效 | 记录原因并进入 DLQ |
| Redis 临时错误 | 保留资格状态，按退避重试或进入补偿 |
| Kafka 暂时不可用 | 请求失败，不能返回受理成功 |
| MySQL 网络或连接池错误 | 不提交 offset，等待重试 |
| MySQL 唯一键冲突且业务订单存在 | 幂等成功并提交 offset |
| MySQL 库存不足 | 进入对账，不能直接当作普通失败 |
| 事务提交结果未知 | 查询业务事实后决定确认或补偿 |

## 8. 验收场景

- 1000 个不同用户并发抢购，库存不超卖。
- 同一用户重复请求只产生一个订单。
- Kafka Consumer 处理成功后、提交 offset 前重启，消息重放不重复创建订单。
- MySQL 暂时不可用时消息不丢失。
- 达到重试次数后进入 DLQ，并可以根据 MySQL 事实恢复或人工标记。
- Redis 资格预占成功但 Kafka 发送失败时，不产生无法解释的永久库存损失。
