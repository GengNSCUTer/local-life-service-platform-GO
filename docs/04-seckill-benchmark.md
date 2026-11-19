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

API、压测客户端、MySQL、Redis 和 Kafka 都在同一台共享开发机上，API 与压测客户端各设置 `GOMAXPROCS=6`。依赖为 MySQL 8.0.33、Redis 7.2.11（AOF 每次写入同步）和 Kafka 4.1.2。每组活动库存为 1000，使用同一批 16,384 个测试用户。结果描述的是包含鉴权、库存判断及售罄和重复响应的 HTTP 请求吞吐。Kafka 投递和 MySQL 写入仅发生在受理成功时，落库结果在测后单独核对。

## 2026 年 11 月 19 日实测

| 总请求数 | 并发客户端 | 耗时 | 请求/秒 | P50 | P95 | P99 | 系统错误 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 256 | 32.30 s | 30,957 | 9 ms | 12 ms | 15 ms | 0 |
| 10,000,000 | 512 | 275.56 s | 36,290 | 14 ms | 19 ms | 23 ms | 0 |

| 总请求数 | 受理 | 售罄 | 重复提交 | 最终确认订单 | 去重用户 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 1,000 | 938,424 | 60,576 | 1,000 | 1,000 |
| 10,000,000 | 1,000 | 9,389,000 | 610,000 | 1,000 | 1,000 |

两组压测结束时，MySQL 和 Redis 的剩余库存均为 0，Redis 待投递队列均为空。
