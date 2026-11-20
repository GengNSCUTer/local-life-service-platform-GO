# 阶段 1 用户、登录和鉴权

沿用 Java 版的手机号验证码接口。首次验证成功会在 MySQL 注册用户，已注册手机号再次登录时复用原用户 ID，每次登录生成一个独立的随机 Token。当前验证码通过本地 API 日志交付，需显式设置 `AUTH_DEV_LOG_CODES=true`。

| 接口 | 登录要求 | 成功结果 |
| --- | --- | --- |
| `POST /user/code?phone=13800000001` | 无 | 生成六位验证码 |
| `POST /user/login` | 无 | 请求体为 `{"phone":"13800000001","code":"123456"}`，返回 `data` 字符串 Token |
| `GET /user/me` | 有 | 返回当前用户的 `id`、`nickName` 和 `icon` |
| `GET /user/:id` | 有 | 查询用户基本资料，用户不存在时 `data` 缺省 |
| `POST /user/logout` | 有 | 撤销当前 Token |
| `POST /voucher-order/seckill/:id` | 有 | `202` 和 `data.orderId`，用订单查询接口跟踪结果 |
| `GET /voucher-order/:id` | 有 | 查询自己的订单 |

`Authorization` 支持原始 Token 和 `Bearer <token>`。参数错误返回 `400`，验证码错误或失效返回 `401`，会话缺失或失效也返回 `401`。同一手机号两次发码至少间隔 30 秒，冷却期间返回 `429`。依赖读写失败返回 `503`。

## 从源码读一遍

1. [app.go](../internal/app/app.go) 创建用户和秒杀模块，复用应用的 MySQL、Redis 客户端。`Register` 用 Gin 路由组给需要登录的接口安装 `RequireLogin`。两个业务模块仍由同一个 API 进程提供服务。
2. [http.go](../internal/user/http.go) 接收请求。`loginRequest` 上的 `` `json:"phone"` `` 是 Go 的 struct tag，`ShouldBindJSON` 用它把 JSON 字段映射到 Go 字段。`context.WithTimeout` 给本次依赖调用设 3 秒上限，`defer cancel()` 在函数返回时释放计时器。
3. [service.go](../internal/user/service.go) 组织发码和登录流程。`crypto/rand` 生成六位码和随机 Token。Redis 的两段 [Lua 脚本](../internal/user/redis.go) 分别负责发码冷却与校验消费。验证码有效期为两分钟，连续输错五次后作废。校验成功和删除发生在同一次脚本执行中，因此同一个验证码同时送往两台 API 时，只有一个请求能通过校验。校验通过后若写库或保存会话失败，需要等冷却结束重新取码。
4. [repository.go](../internal/user/repository.go) 用 `db.WithContext(ctx)` 将请求截止时间传给 GORM。`tb_user.phone` 的唯一索引负责拦住重复注册，遇到 MySQL 重复键错误时再读已经存在的用户。`account` 用于写数据库，HTTP 响应的 `profile` 只包含可以公开给已登录用户的字段。
5. `login` 使用 Redis `MULTI/EXEC` 一起写入 Hash 和 30 分钟的 TTL。后续 `RequireLogin` 从 `login:token:<token>` 恢复用户，剩余时间进入最后 15 分钟才续回 30 分钟。`context.WithValue` 将本次请求的身份传给下游，秒杀的 `submit` 通过 `user.CurrentID(ctx)` 取用户 ID。这样两个 API 实例可以共享同一份登录态，登出在哪台执行都能让其他实例失去这个 Token。旧的秒杀压测会话也能读取，并保留其原 TTL。
6. 秒杀使用恢复出的用户 ID 预占库存，并把订单事件写入 Redis outbox。Kafka 消费者最终写入 MySQL。登录流程本身只依赖 Redis 和 MySQL，Kafka 仍服务于异步下单。

## 本地体验

按 [README](../README.md#运行开发环境) 启动依赖。临时数据库按编号应用三份 SQL。已有秒杀表只需执行用户迁移。已有 Java 版 `tb_user` 时，迁移会保留该表和数据。

```bash
docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/003_user.sql
AUTH_DEV_LOG_CODES=true go run ./cmd/all
```

另开一个终端，先发验证码，再从 API 日志里的 `development login code` 读取 `code` 字段。

```bash
base=http://127.0.0.1:8080
phone=13800000001
curl -i -X POST "$base/user/code?phone=$phone"
read -r -p '验证码: ' code
curl -i -H 'Content-Type: application/json' \
  -d "{\"phone\":\"$phone\",\"code\":\"$code\"}" "$base/user/login"
read -r -s -p '复制响应 data 中的 Token: ' token
printf '\n'
curl -i -H "Authorization: Bearer $token" "$base/user/me"
```

按 [秒杀联调文档](03-seckill-design.md#本地体验) 准备活动。在开放时间用同一个 Token 发起秒杀。可用相同依赖配置，以 `HTTP_ADDR=127.0.0.1:8081 go run ./cmd/api` 启动第二个进程，再用原 Token 查询当前用户或订单。

```bash
curl -i -X POST -H "Authorization: Bearer $token" "$base/voucher-order/seckill/42"
curl -i -H "Authorization: Bearer $token" http://127.0.0.1:8081/user/me
curl -i -X POST -H "Authorization: Bearer $token" http://127.0.0.1:8081/user/logout
curl -i -H "Authorization: Bearer $token" "$base/user/me"
```

最后一次请求应返回 `401`。也可以在登出前等待自然过期，或在隔离的开发 Redis 中对会话执行 `PEXPIRE` 并等待其过期。

## 复现验证码并发测试

将 `HMDP_AUTH_TEST_REDIS_ADDR` 指向隔离的开发 Redis。需要密码时沿用 `REDIS_PASSWORD`。测试创建有 TTL 的临时键，分别通过两个 Redis 客户端提交共 20 次校验，检查成功次数为 1，并确认成功登录以后发码冷却仍然生效。

```bash
HMDP_AUTH_TEST_REDIS_ADDR=127.0.0.1:26379 go test -race ./internal/user -run TestVerificationCodeCanOnlyBeConsumedOnce -count=1 -v
```

## 本轮联调结果

在独立的 MySQL、Redis 和 Kafka 环境中启动两个共享依赖的 API 实例，验证了验证码超时、跨实例发码冷却、五次输错后作废，以及 20 次并发提交同一码只成功一次。两个实例能读取同一会话，且登出只撤销指定 Token。将会话 TTL 调短后验证了按需续期和过期后的 `401`，停止 Redis 后验证了鉴权返回 `503`。

用真实登录 Token 提交秒杀请求后，在另一实例查询到 `confirmed`，MySQL 的订单用户 ID 与注册用户一致。旧压测会话和含 NULL 资料字段的 Java 用户也通过了兼容验证。
