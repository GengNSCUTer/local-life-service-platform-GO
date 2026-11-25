# 社交通知推拉结合

普通作者发帖时，在 MySQL 同一事务内写博客和 Outbox。Worker 通过 Kafka 读事件，按每批 512 人的游标查询写入各粉丝的 Redis 通知集合。用户读取时，服务对照当前关注关系过滤结果。

发帖时从 MySQL 的 `social_follows` 判断是否达到粉丝阈值。默认 `SOCIAL_PULL_MIN_FOLLOWERS=10000`，正好 10,000 名及以上走拉模式，低于阈值走推送。设置 `0` 可让所有新帖走推送。所有 API 实例使用同一阈值。之前的作者名单 `SOCIAL_PULL_AUTHOR_IDS` 已移除。

查询用 `idx_followers` 定位该作者的关注关系，判断阈值对应的第 N 条是否存在。使用默认阈值时，即使有百万粉丝也只需查到第 10,000 条。模式在写博客的事务中决定并保存在 `tb_blog.notification_mode`。拉模式只写博客，推模式同时写 Outbox。关注或取关完成后，作者下一次发帖会按照当时的关系重新判断，已有帖子继续沿用各自的模式。已应用 `012_social_notification_mode.sql` 的数据库可以直接升级。 [发帖事务](../internal/social/blog.go)

`GET /blog/notifications` 读取 Redis 推送候选，并从 MySQL 查询近 30 天内已关注作者的读时投递候选。按博客 ID 去重、降序返回最多 100 条。读时投递按当前关注关系计算，近期文章可能在新关注时立即出现。关注动态接口继续走自己的 MySQL 游标分页。

## 本机复现

在已应用迁移的隔离库运行 `HMDP_SOCIAL_TEST_DSN='...' go test -race ./internal/social -run TestPublishChoosesModeFromCurrentFollowers -count=1`，可验证重复关注、跨过阈值、低于阈值，以及旧帖保留原模式。

设置 `MYSQL_BIN_DIR`、`REDIS_BIN_DIR` 和 `KAFKA_BIN_DIR`。依次运行以下用例，每轮重新创建数据，使用 3 条热门作者帖子、1 条普通作者帖子和每秒 20 次通知读取。统计前 200 次读取的 P99、头尾粉丝可见时间、普通作者可见时间、Redis ZADD 次数、Kafka lag 及 Outbox 清空结果，并核对每篇帖子的投递模式。

```bash
SOCIAL_TEST_FOLLOWERS=100000 SOCIAL_PULL_MIN_FOLLOWERS=0 scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=100000 SOCIAL_PULL_MIN_FOLLOWERS=10000 scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=1000000 SOCIAL_PULL_MIN_FOLLOWERS=0 scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=1000000 SOCIAL_PULL_MIN_FOLLOWERS=10000 scripts/stage7-local.sh social
```

## 3090 对照结果

以下为自动判定上线前的推拉对照。同一台 3090 上每轮启动一个 API、一个 Worker、MySQL 8.0.33、Redis 7.2.11 和 Kafka 4.1.2。Redis 启用 AOF `always`，Kafka 为单 Broker 三分区。每轮重新建立合成数据，以下时间从各自发帖请求开始计算。

| 热门作者粉丝 | 模式 | 第三篇到尾部样本 | 普通作者通知可见 | Redis ZADD | 首 200 次读 P99 |
| --- | --- | ---: | ---: | ---: | ---: |
| 100,000 | 推送 | 67.605 秒 | 67.588 秒 | 300,002 | 250.256 ms |
| 100,000 | 推拉结合 | 0.064 秒 | 1.757 秒 | 2 | 5.018 ms |
| 1,000,000 | 推送 | 115.866 秒 | 115.912 秒 | 3,000,002 | 15.770 ms |
| 1,000,000 | 推拉结合 | 0.064 秒 | 0.409 秒 | 2 | 4.724 ms |

推送组有数据的 Kafka 分区最终 lag 为 0，采样最大单分区 lag 为 2，部分早期采样的 offset 尚未提交。推拉组采到并在结束时核对的 lag 为 0。四轮均通过头尾样本、通知排序、去重、取关及 MySQL Outbox 归零检查。普通作者的两个关注者仍收到推送。每组运行一次，主机负载会波动。读流量来自三个轮询账号，每秒约 20 次，百万指关注关系数。

另做了一轮兼容性演练。先合并一条旧推送和一条读时投递，再向 Kafka 重放两次旧推送事件并启动 Worker。消费积压归零，通知仍各显示一条。

## 自动判定回归

在 3090 上以默认阈值 10,000、100 万条热门作者关注关系复跑。`EXPLAIN ANALYZE` 确认使用 `idx_followers` 的覆盖索引读取 10,000 条，取到阈值记录耗时 3.78 ms。热门作者的三篇新帖全部标记为拉模式，首尾样本均可见。普通作者的一篇帖子走推送，Redis 增加 2 次 `ZADD`，对应其两位粉丝。Kafka 已使用分区的 lag、MySQL Outbox 和待处理分发任务最终均为 0。

三个账号共轮询 201 次通知，前 200 次读取 P99 为 7.46 ms。百万是关注关系数，本轮没有模拟百万在线用户。
