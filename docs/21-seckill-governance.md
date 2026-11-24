# 秒杀链路观测、流量保护和库存补偿

## 追踪一次真实订单

给 API 与 Worker 配置 `TRACE_OTLP_ENDPOINT`，指向 OTLP HTTP 接收端的 `/v1/traces`。例如在本机 Jaeger 或 OpenTelemetry Collector 启用 OTLP HTTP 后配置 `http://127.0.0.1:4318/v1/traces`。`TRACE_SAMPLE_RATIO` 默认 `0.01`，本机联调可设 `1`。

Gin 给秒杀 POST 建立服务端 Span，通过 `context.Context` 传给 Redis 预占调用。采样到的预占把 W3C `traceparent` 存入已有 JSON 事件。Relay 从预占还原上下文，并在发送 Kafka 时写入 Producer Span 的追踪 Header。Consumer 恢复后创建消费和 MySQL 事务 Span。HTTP 日志对采样请求同时记录 Request ID 和 Trace ID。旧事件可省略 Header，导出通过有界缓冲在后台完成。

## 两个入口的限流与店铺熔断

- `AUTH_CODE_IP_LIMIT=20` 设置验证码入口每 60 秒的连接 IP 配额。此策略直接读取 TCP 对端地址。若部署在统一反向代理后，应根据实际网关配置选择限流位置，避免所有用户共享代理地址的配额。原有手机号冷却继续生效。
- `SECKILL_USER_LIMIT=0` 默认关闭。配置正整数后，在鉴权之后按用户限制秒杀提交，每 60 秒一个计数窗口。Redis Lua 合并计数和设置过期，多个 API 实例共用同一份配额。超限返回 `429` 和 `Retry-After`，限流 Redis 不可用时返回 `503`。秒杀队列背压继续保护预占和异步处理。
- `SHOP_MYSQL_BREAKER=true` 为每个 API 的店铺详情回源开启熔断。单次 MySQL 回源最长 500 ms，连续 5 次依赖失败后打开 5 秒，到期只允许一次探测。Redis 命中直接返回，未知店铺、调用方取消均不计为依赖失败。

Prometheus 指标为 `hmdp_http_rate_limit_total` 和 `hmdp_shop_mysql_circuit_state`。状态值 `0`、`1`、`2` 对应关闭、打开、半开。

## 按订单预览与重试库存补偿

先应用 `013_seckill_repair_attempts.sql`，然后在能连接目标 MySQL 和 Redis 的运行环境执行。

```bash
./api repair-seckill -order-id "$ORDER_ID"
./api repair-seckill -order-id "$ORDER_ID" -apply
```

预览返回 `ready`、`ready_to_sync`、`already_synced` 或 `manual_review`。执行需要 MySQL 中的订单已取消、有未同步的确定性补偿记录，且订单归属分片与活动相符。Redis Lua 写入之前重查库存、买家及队列类型。库存丢失、归属不清、补偿原因不明确交给人工复核。执行前写入 `seckill_repair_attempts`，退出时填入结果。`started` 表示结果尚未写入，需先预览当前状态。库存只经现有补偿 Lua 回补，同一订单重复操作沿用原幂等标记。

## 本机验收

两台本机 API 共享 Redis，分别耗用同一个 IP 和同一个用户的配额，第三次均返回 `429`。给 MySQL 店铺表加写锁触发回源超时，熔断打开期间仍能读取已缓存店铺，释放锁后探测恢复。一笔真实订单导出了 HTTP、Redis、Outbox、Kafka 消费和 MySQL 共五个有父子关系的 Span。停止 OTLP 接收端后继续提交，订单仍在 MySQL 确认。

模拟取消事务已提交、Redis 尚未回补。预览保持库存不变，执行加回一份，重复执行返回 `already_synced`。移除另一笔取消订单的 Redis 买家记录后，执行返回 `manual_review` 并保留原库存。审计表记录了三个操作结果。

另向 Kafka 投递一条没有追踪字段和 Header 的 v1 订单事件，升级后的 Worker 正常将订单写为 `confirmed`。

保护开关的开销用同机两组 API 对照。每路径和每组记录 1,000 次请求，预热 100 次，两组共用 MySQL 和 Redis，按约每组 100 请求/秒交替请求。Redis AOF 为 `always`。抢券售罄路径开启单用户限流后 P99 从 1.459 ms 到 3.086 ms。不存在的店铺每次用不同 ID 触发 MySQL 回源，开启熔断后 P99 从 3.685 ms 到 3.718 ms。该对照用于观察正常依赖下的请求开销。
