# 阶段 3 优惠券、活动和订单查询

| 接口 | 权限 | 行为 |
| --- | --- | --- |
| `GET /voucher/list/:shopId` | 公开 | 列出店铺上架优惠券，Go 创建的秒杀券带库存和时间窗口 |
| `POST /voucher` | 优惠券管理员 | 创建普通券，返回优惠券 ID |
| `POST /voucher/seckill` | 优惠券管理员 | 创建优惠券和秒杀活动，预热库存，返回优惠券 ID |
| `POST /voucher/seckill/:id/initialize` | 优惠券管理员 | 重新尝试初始化已创建活动的 Redis 库存 |
| `POST /voucher-order/seckill/:id` | 登录用户 | 提交秒杀，预占成功返回 `202` 和 `orderId` |
| `GET /voucher-order/:id` | 订单所属用户 | 查询待处理或已持久化订单 |
| `GET /voucher-order?current=1` | 登录用户 | 查询自己的 MySQL 订单历史，每页 10 条 |

普通券支持创建和展示。订单历史覆盖当前系统已持久化的秒杀订单。活动创建时配置库存和时间，活动开始后 Redis 只允许在原窗口内抢券。历史 Java 普通券可直接列表查询，旧 Java 秒杀活动数据需单独迁移才能在本系统发售。

## 从源码跟一次请求

1. Gin 校验 Token 后，`requireAdmin` 通过请求 `Context` 取用户 ID，并核对 `VOUCHER_ADMIN_USER_ID`。`createHandler` 解析 JSON。金额以分为单位，活动时间使用带时区的 RFC 3339 字符串。Go 把时间统一成 UTC，截断到 MySQL 保存的毫秒精度。 [接口](../internal/voucher/http.go) · [活动参数](../internal/voucher/service.go)
2. GORM 的 `Transaction(func(tx *gorm.DB) error {...})` 把优惠券、活动和两者的关联放进同一个 MySQL 事务。回调返回错误时回滚。创建接口可带 `Idempotency-Key`，同一个键和同样的参数会复用第一次写入的 ID，换参数会收到 `409`。并发请求由 MySQL 的主键和事务协调。 [数据访问](../internal/voucher/repository.go)
3. MySQL 事务提交后，Go 调用秒杀模块预热 Redis。Lua 发现相同活动已经初始化时保留剩余库存。Redis 初始化失败时，响应 `503` 的 `data` 会带已落库的优惠券 ID，可在开售前调用初始化接口，或带同一个创建请求键重试。开售后如果 Redis 库存键缺失，初始化会返回 `409`，避免按总库存重新开售。 [预热与抢券脚本](../internal/seckill/redis.go)
4. 用户抢券时，Redis Lua 用 Redis 的时间检查活动窗口，核对一人一单并预占库存。成功后由 Worker 通过 Kafka 异步写 MySQL。详情接口可查 `pending`，历史接口从 MySQL 查询。MySQL 使用条件更新和唯一索引落单，最后一件卖完后，同一订单消息重放仍可识别为已处理。 [秒杀接口](../internal/seckill/http.go) · [落单事务](../internal/seckill/store.go)

## 本地体验

新库按编号应用迁移 `001` 至 `006`。已经完成阶段 3 时只需执行订单生命周期迁移。应用的秒杀 Worker 会在后台创建所需 Kafka 主题，新业务活动可直接通过 HTTP 创建。原来的 `prepare-seckill` 命令用于隔离环境的压测和故障演练。

```bash
docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/005_voucher.sql
docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/006_order_lifecycle.sql
SHOP_ADMIN_USER_ID=7 VOUCHER_ADMIN_USER_ID=7 AUTH_DEV_LOG_CODES=true go run ./cmd/api
```

将示例里的 `7` 换成自己的用户 ID。用管理员 Token 创建活动，把 `shopId` 换成现有店铺 ID。

```bash
base=http://127.0.0.1:8080
read -r -s -p '管理员 Token: ' admin_token
printf '\n'
begins=$(date -u -d '+45 seconds' +%FT%TZ)
ends=$(date -u -d '+15 minutes' +%FT%TZ)
key="tea-shop-$(date +%s)"
curl -sS -H "Authorization: Bearer $admin_token" -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' \
  -d "{\"shopId\":1,\"title\":\"茶馆秒杀券\",\"payValue\":500,\"actualValue\":1000,\"stock\":1,\"beginTime\":\"$begins\",\"endTime\":\"$ends\"}" \
  "$base/voucher/seckill"
curl -fsS "$base/voucher/list/1"
```

记录创建返回的 `data`。到开始时间后，用任意已登录的账号抢券，然后查看订单。

```bash
read -r -p '优惠券 ID: ' voucher_id
read -r -s -p '买家 Token: ' buyer_token
printf '\n'
curl -i -X POST -H "Authorization: Bearer $buyer_token" "$base/voucher-order/seckill/$voucher_id"
read -r -p '返回的 orderId: ' order_id
curl -fsS -H "Authorization: Bearer $buyer_token" "$base/voucher-order/$order_id"
curl -fsS -H "Authorization: Bearer $buyer_token" "$base/voucher-order?current=1"
```

## 本轮验收

本地启动两台 API 及独立的 MySQL、Redis、Kafka。两台 API 并发提交 12 次相同的创建请求，产生一条活动记录。开售后同一用户并发提交 16 次，只有一次预占成功。验证了未开始、已结束、售罄、订单归属检查、Kafka 落单和订单分页。故障注入让 Redis 预热失败，恢复后通过相同请求键重试，复用了原优惠券 ID。MySQL 回归测试覆盖最后一份库存扣完后同一订单消息的再次投递。

```bash
# 将 HMDP_SECKILL_TEST_DSN 设置为应用过 001_seckill.sql 的隔离数据库连接字符串。
go test -race ./internal/seckill -run TestPersistOrderCanBeReplayedAfterStockReachesZero -count=1 -v
```
