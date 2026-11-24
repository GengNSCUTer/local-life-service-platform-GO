# 社交通知推拉结合

普通作者发帖时，在 MySQL 同一事务内写博客和 Outbox。Worker 通过 Kafka 读事件，按每批 512 人的游标查询写入各粉丝的 Redis 通知集合。用户读取时，服务对照当前关注关系过滤结果。

已有库先应用 `012_social_notification_mode.sql`，再更新 API 和 Worker。默认所有作者仍走批量推送。两个进程升级后，在所有 API 实例设置同一份 `SOCIAL_PULL_AUTHOR_IDS`，例如 `2,7`。名单内作者的**新文章**标为读时投递，在发帖时只写博客。旧文章继续沿用原先的投递方式。移除名单条目仅影响后续文章，已有读时投递文章仍可在通知里读取。

`GET /blog/notifications` 读取 Redis 推送候选，并从 MySQL 查询近 30 天内已关注作者的读时投递候选。按博客 ID 去重、降序返回最多 100 条。读时投递按当前关注关系计算，近期文章可能在新关注时立即出现。关注动态接口继续走自己的 MySQL 游标分页。

## 本机复现

设置 `MYSQL_BIN_DIR`、`REDIS_BIN_DIR` 和 `KAFKA_BIN_DIR`。依次运行以下用例，每轮重新创建数据，使用 3 条热门作者帖子、1 条普通作者帖子和每秒 20 次通知读取。统计前 200 次读取的 P99、头尾粉丝可见时间、普通作者可见时间、Redis ZADD 次数、Kafka lag 及 Outbox 清空结果。

```bash
SOCIAL_TEST_FOLLOWERS=100000 SOCIAL_PULL_AUTHOR_IDS= scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=100000 SOCIAL_PULL_AUTHOR_IDS=2 scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=1000000 SOCIAL_PULL_AUTHOR_IDS= scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=1000000 SOCIAL_PULL_AUTHOR_IDS=2 scripts/stage7-local.sh social
```
