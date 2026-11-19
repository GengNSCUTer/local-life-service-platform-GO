# 阶段 2 店铺、分类和详情缓存

店铺表提供商圈、地址、坐标、营业时间和人均消费。查询接口沿用 Java 版路径和 JSON 字段。附近商户按距离查询在阶段 5 实现。

| 接口 | 行为 |
| --- | --- |
| `GET /shop-type/list` | 按 `sort`、`id` 升序列出分类 |
| `GET /shop/:id` | 查店铺详情，查无记录返回 `404` |
| `GET /shop/of/type?typeId=1&current=1` | 按分类分页，每页 5 家 |
| `GET /shop/of/name?name=茶&current=1` | 按名称子串分页，每页 10 家，名称可省略 |
| `POST /shop` | 管理员创建店铺，返回新 ID |
| `PUT /shop` | 管理员传 `id` 和需要修改的字段 |

分页默认从第 1 页开始，按 `id` 排序。维护接口先验证登录，再核对 `SHOP_ADMIN_USER_ID`，默认值 `0` 关闭写入权限。更新时省略的字段保持原值，JSON `null` 按省略处理。管理接口可以设置营业时间和均价，评分、销量和评论数由业务数据维护。

## 顺着 Go 源码读

1. Gin 的 `Register` 把读取和维护路由放在同一个 `/shop` 路由组里。维护子组追加阶段 1 的 `RequireLogin`，再检查当前请求 `Context` 中的用户 ID。 [路由和权限](../internal/shop/http.go)
2. `get` 解析路径里的店铺 ID，再把带 3 秒截止时间的 `Context` 交给 `detail`。详情先读 Redis `cache:shop:<id>`，命中 JSON 就返回。未命中时用 GORM 查 MySQL，查到后写回 Redis。正缓存保留 5 分钟，再加 0 到 59 秒随机时间，让一批热键错开过期。 [详情和缓存](../internal/shop/service.go)
3. 数据库明确查不到时写入 `null`，保留 30 秒。同一 ID 反复被查也能快速返回 `404`。MySQL 报错时返回 `503`，并跳过缓存写入。回源前用容量为 1 的 channel 控制并发，它像一张通行证。拿到通行证的请求再查一次缓存。前面的请求已经回填时，后面就直接返回。通行证在单个 API 进程内共享。 [回源逻辑](../internal/shop/service.go)
4. 管理员更新时，GORM 在事务内校验分类并提交 MySQL。提交后写入一个短期失效标记。详情回填的 Lua 会比较读库前看到的值，更新留下新标记时，较早开始的旧读请求不能覆盖 Redis。Redis 写入失效标记失败时，数据库更新仍成功，旧缓存会在原 TTL 后过期，日志记录缓存失效失败。 [事务](../internal/shop/repository.go) · [失效与回填](../internal/shop/service.go)

## 本地联调

按 README 启动依赖并应用已有迁移。阶段 2 新增的迁移创建店铺和分类表，已有 Java 表时保留其数据。空库可先加入一条开发分类。

```bash
docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/004_shop.sql
```

```sql
INSERT INTO tb_shop_type (name, sort) VALUES ('餐饮', 1);
```

先按阶段 1 文档登录，用 `GET /user/me` 拿到用户 ID。启动 API 时将 `SHOP_ADMIN_USER_ID` 设为该 ID，以下示例以 `7` 为例。分类 ID 从分类列表取，示例使用 `1`。

```bash
SHOP_ADMIN_USER_ID=7 AUTH_DEV_LOG_CODES=true go run ./cmd/api
```

```bash
base=http://127.0.0.1:8080
curl -fsS "$base/shop-type/list"
read -r -s -p 'Token: ' token
printf '\n'
curl -fsS -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  -d '{"name":"Go 茶馆","typeId":1,"address":"示例路 1 号","x":120.15,"y":30.31}' "$base/shop"
read -r -p '新店铺 ID: ' shop_id
curl -fsS "$base/shop/$shop_id"
docker compose exec -T redis redis-cli TTL "cache:shop:$shop_id"
curl -fsS -X PUT -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  -d "{\"id\":$shop_id,\"openHours\":\"09:00-22:00\"}" "$base/shop"
curl -fsS "$base/shop/$shop_id"
```

可以对隔离的 Redis 运行并发回填的回归测试。需要密码时沿用 `REDIS_PASSWORD`。

```bash
HMDP_SHOP_TEST_REDIS_ADDR=127.0.0.1:26379 go test -race ./internal/shop -run TestInvalidationRejectsAnOlderCacheFill -count=1 -v
```

## 本轮验收

本地使用两个 API 实例、MySQL、Redis 和 Kafka 联调。验证了 Java 原版店铺数据读取、按分类分页、按名称查询、24 个并发详情请求、正缓存和空值缓存。管理员创建和局部更新通过，匿名请求和普通用户分别返回 `401` 和 `403`。从另一台实例读取更新后的店铺，并用真实 Redis 测试旧读请求的回填被拦截。

停止 MySQL 后，已缓存的详情仍然返回 `200`，未缓存的详情和店铺列表返回 `503`。真实 Token 抢券和 Kafka 消费落单也通过回归。
