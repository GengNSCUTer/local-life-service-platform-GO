# 中间件职责设计

## 0. Redis 和 Kafka 的分工

两者都会进入 Go 版本的正式链路，但解决的问题不同：

| 组件 | 主要职责 | 是否直接决定请求结果 |
| --- | --- | --- |
| Redis | 登录态、热点缓存、库存预占、重复购买判断、Lua 原子操作、Feed 和统计结构 | 是，原子预占并登记待发事件后返回受理结果 |
| Kafka | 订单事件持久化、异步削峰、消费者组分发、失败重试和死信 | 处理异步订单事件，可暂时积压在 Redis 待发队列 |
| MySQL | 用户、商户、优惠券、订单等最终业务事实 | 是，订单最终状态以 MySQL 事务结果为准 |

秒杀请求的同步路径是 `Gin → Redis Lua`。订单的异步路径是 `Redis 待发记录 → Kafka → MySQL 事务`。

## 1. Gin

Gin 负责路由和 HTTP 中间件链。全局中间件只放跨所有请求的能力，业务鉴权放到需要登录的路由组。

建议顺序：

1. Request ID
2. Recovery
3. Access Log
4. Metrics
5. Timeout
6. Token Restore
7. Login Required

Token Restore 负责从 `authorization` 请求头恢复用户并刷新 Redis TTL。Login Required 只检查 Context 中是否有用户。公开接口只挂 Token Restore，不挂 Login Required。

## 2. GORM 和 MySQL

GORM 负责基础 CRUD、查询条件、事务和连接池管理。秒杀订单的关键写入不使用自动保存关联对象，而是明确执行：

1. 在事务内条件扣减 `seckill_activities.remaining_stock`。
2. 插入 `seckill_orders`。
3. 处理唯一键冲突和事务回滚。

需要补充或确认的数据库约束：

- `tb_user.phone` 唯一。
- `seckill_orders(voucher_id, user_id)` 唯一，作为一人一单的最终防线。
- 订单 ID 主键唯一。
- 关注关系根据业务需要增加 `(user_id, follow_user_id)` 唯一。

不使用 `AutoMigrate` 修改已有业务库。表结构变更通过版本化 SQL Migration，便于复现和审查。

## 3. go-redis

Redis 客户端统一由 platform 层创建，业务层使用封装后的接口或脚本对象。脚本按功能放在 `internal/platform/redis/scripts` 或对应业务目录，由启动阶段加载并缓存 SHA。

Redis 使用边界：

- 登录态和验证码：Hash、String、TTL。
- 店铺缓存：String、空值缓存和互斥锁。
- 秒杀资格与待发事件使用 Lua、Hash、String 和 Sorted Set。
- Feed 和点赞：Sorted Set。
- 关注：Set。
- 附近商户：GEO。
- 签到：Bitmap。
- UV：HyperLogLog。

所有 Redis 操作必须接受 `context.Context`。超时和取消不能被业务代码吞掉。Lua 返回值转换为明确的领域错误，例如 `ErrOutOfStock`、`ErrAlreadyOrdered` 和 `ErrActivityNotInitialized`。

## 4. Kafka

Kafka 负责秒杀订单事件的异步削峰和可靠消费。Topic 初始规划：

| Topic | 用途 |
| --- | --- |
| `hmdp.seckill.order.v1` | 秒杀订单事件 |
| `hmdp.seckill.order.dlq.v1` | 无效的消息和确定的订单冲突 |

消息包含稳定的 `order_id`、`user_id`、`voucher_id`、创建时间和 schema 版本。首版每个订单只有一个事件，因此 `order_id` 也作为事件 ID 和 Kafka 消息键。消费者通过订单 ID 和业务唯一键实现幂等。

Kafka Consumer 的处理顺序：

1. 读取消息。
2. 反序列化并校验 schema。
3. 在 MySQL 事务中执行订单写入。
4. 事务成功或确认业务重复后提交 offset。
5. 可重试错误进入退避流程。
6. 不可恢复错误进入 DLQ，并记录原因。

首版以至少一次投递为目标。MySQL 暂时失败时，消费者保留未提交的 offset 并退避重试。库存不一致时，数据库订单记录进入人工核对状态。

## 5. 日志、指标和追踪

首版先使用结构化日志，统一记录：

- `request_id`
- `user_id`
- `order_id`
- `voucher_id`
- `kafka_topic`
- `partition`
- `offset`
- `duration_ms`
- `error_code`

秒杀和 Kafka 指标至少包括：

- HTTP 请求总数、成功数、业务失败数和 P99 延迟。
- Redis Lua 执行错误和库存不足次数。
- Kafka Producer 错误、Consumer lag、重试数和 DLQ 数。
- MySQL 事务成功、回滚、唯一键冲突和库存不足次数。
- 当前处理中的 goroutine 和优雅停机耗时。
