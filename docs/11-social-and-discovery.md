# 阶段 5 博客、关注、附近店铺和签到

阶段 4 的开发库执行一次 `008_social.sql`，新库按编号执行 `001` 到 `008`。迁移复用已有 `tb_blog`，为历史博客保留展示用点赞数。个人点赞和关注关系写入 Go 版关系表，旧 Java 的 Redis 点赞记录与 `tb_follow` 关系需要单独导入。

## 接口

| 接口 | 用法 |
| --- | --- |
| `POST /blog` | 登录后传 `shopId`、`title`、`content` 和可选的 `images` |
| `GET /blog/:id`、`GET /blog/hot` | 博客详情和热门列表，携带 Token 时返回个人点赞状态 |
| `GET /blog/of/me`、`GET /blog/of/user?id=123` | 按 `current` 查询个人或指定作者的博客 |
| `PUT /blog/like/:id?liked=true`、`DELETE /blog/like/:id` | 幂等地点赞和取消点赞。兼容旧客户端的 `PUT /blog/like/:id` 切换用法 |
| `GET /blog/likes/:id` | 最近点赞的 5 位用户 |
| `PUT /follow/:id/true`、`PUT /follow/:id/false` | 登录后关注或取关 |
| `GET /follow/or/not/:id`、`GET /follow/common/:id` | 查询关注状态与共同关注 |
| `GET /blog/of/follow` | 登录后查询关注动态，每页 10 条。下一页传 `cursor=nextCursor` |
| `GET /blog/notifications` | 查询当前关注对象的近期发帖通知，最多 100 条 |
| `GET /shop/of/type?typeId=1&x=120.1&y=30.2&current=1` | 5 公里内同类型店铺，按距离升序，每页 5 家，`distance` 单位为米 |
| `POST /user/sign`、`GET /user/sign/count` | 登录后签到和查询截至当天的本月连续签到天数，按北京时间划日 |

关注动态保留 `list`、`minTime` 和 `offset` 响应字段。`minTime` 在 Go 版是本页最后一条博客 ID，可作为旧客户端下一页的 `lastId`，`offset` 固定为 0。

## 从源码读一次请求

1. `changeLike` 在 GORM 事务中锁住博客行，读取当前用户是否点赞，再决定是否更新关系与点赞数。重复发送目标状态时，两者保持一致。 [博客实现](../internal/social/blog.go)
2. `feed` 把关注关系和博客联表查询，以 `b.id < cursor` 读取下一页。两条博客即使在同一秒发布，也能通过不同的 ID 确定顺序。请求下一页期间新发布的博客会出现在刷新后的第一页。 [动态接口](../internal/social/http.go)
3. `commonFollows` 使用 Redis Set 求交集。MySQL 事务更新关注关系和缓存版本，后续请求读取新版本。缓存缺失时重建，Redis 失败时通过 MySQL 联表查询。 [关注实现](../internal/social/follow.go)
4. 发布博客和写 Outbox 同时提交。Relay 收到 Kafka 确认后删除 Outbox，消费者分页获取当时的关注者，把博客 ID 写入各自的 Redis Sorted Set。重新收到同一事件时，同一个成员只保留一份。查通知时核对当前关注关系。 [通知 Worker](../internal/social/notifications.go)
5. 附近查询用 `GEOSEARCH` 查 ID 和距离，然后批量查 MySQL 补齐详情。店铺创建、坐标修改与分类变更会在同一 MySQL 事务里更新索引版本，下一次查询按需重建。 [附近店铺](../internal/shop/geo.go)
6. 签到将本月第 `day - 1` 位设为 1。读取本月截至当天的位段后，从最后一位开始数连续的 1。当天重复签到只会设置同一位，跨月从新 Bitmap 开始。 [签到实现](../internal/user/sign.go)

## 联调

在隔离的 MySQL、Redis、Kafka 上通过 HTTP 验证博客、分页、并发幂等点赞、取关和通知过滤。暂停 Kafka 后发布博客，检查 MySQL Outbox，并在 Kafka 恢复后核对 Redis 通知。通过 Redis 读取地理索引和签到位，核对 HTTP 查询结果。
