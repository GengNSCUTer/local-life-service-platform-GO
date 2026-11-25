# 社交通知推拉结合

普通作者发帖时，在 MySQL 同一事务内写博客和 Outbox。Worker 通过 Kafka 读事件，将推送拆成持久化任务。每个分片按 512 人一批写入 Redis 通知集合。

发帖时从 MySQL 的 `social_follows` 判断是否达到粉丝阈值。默认 `SOCIAL_PULL_MIN_FOLLOWERS=10000`，正好 10,000 名及以上走拉模式，低于阈值走推送。设置 `0` 可让所有新帖走推送。所有 API 实例使用同一阈值。之前的作者名单 `SOCIAL_PULL_AUTHOR_IDS` 已移除。

查询用 `idx_followers` 定位该作者的关注关系，判断阈值对应的第 N 条是否存在。使用默认阈值时，即使有百万粉丝也只需查到第 10,000 条。模式在写博客的事务中决定并保存在 `tb_blog.notification_mode`。拉模式只写博客，推模式同时写 Outbox。关注或取关完成后，作者下一次发帖会按照当时的关系重新判断，已有帖子继续沿用各自的模式。已应用 `012_social_notification_mode.sql` 的数据库可以直接升级。 [发帖事务](../internal/social/blog.go)

`GET /blog/notifications` 以 MySQL 当前关注关系为准，合并推送和拉取的近 30 天帖子，按发布时间及博客 ID 降序返回至多 100 条。查询先用覆盖索引取每位作者至多 100 个候选 ID，然后读取合并后的帖子详情。读取时按用户回填本页涉及的推送通知，超时预算 50 毫秒。Redis 通知数据为空、部分丢失或不可访问时，MySQL 仍可返回完整结果。

用户新关注后，可以看到该作者近 30 天的帖子。取关立即排除该作者，并可用更早的有效帖子补满 100 条。后台推送使用任务建立时记录的粉丝 ID 范围，逐批读取仍存在的关注关系。范围内刚加入的用户能否收到异步写入取决于已处理游标，所有当前粉丝在通知页看到的结果采用同一规则。关注动态接口保留自己的游标分页。

## 升级和运行

暂停社交 Worker 后执行迁移 `017`，再升级 API 和 Worker。迁移保留仍在通知保留期内的旧任务去重记录和未完成游标，旧的任务表随迁移清空。原有待处理任务继续从迁移前的游标执行。所有进程升级后恢复流量。

- 新事件在一个事务里创建任务与分片，提交后才确认 Kafka offset。粉丝至少 4096 时按用户 ID 区间切成 8 个分片，空区间即时完成。每个 Worker 独立领取分片，一次处理一批。用户 ID 分布不均时，各分片工作量可能有差异。
- 一个批次连续失败 8 次后，分片进入失败状态。每次重试使用指数退避，已有进度保存在分片游标中。日志带分片和异常信息，`hmdp_social_fanout_failed_shards` 显示待处理失败分片数。
- Worker 按小批量清理 37 天前的任务及分片。早于 30 天的帖子重放时不会创建新任务，清理后仍有保留期校验保护。
- `SOCIAL_NOTIFICATION_REDIS_ADDR` 可指向独立的通知 Redis，API 和 Worker 使用同一地址。默认共用主 Redis。若主 Redis 同时承担会话并完全不可用，登录鉴权仍会受影响。`hmdp_social_notification_cache_total` 统计回填和缓存不可用。

排除失败原因后，用以下 SQL 查看和重新投递一个分片。将 `@blog_id` 与 `@shard` 换成实际值，重投从保留的游标继续。

```sql
SELECT blog_id, shard, last_follower, end_follower, attempts, failed_at
FROM social_fanout_shards WHERE failed_at IS NOT NULL ORDER BY failed_at;

UPDATE social_fanout_shards AS s JOIN social_fanout_jobs AS j ON j.blog_id = s.blog_id
SET s.failed_at = NULL, s.attempts = 0, s.lease_token = NULL, s.due_at = CURRENT_TIMESTAMP(3)
WHERE s.blog_id = @blog_id AND s.shard = @shard AND s.failed_at IS NOT NULL
  AND j.created_at >= TIMESTAMPADD(DAY, -30, CURRENT_TIMESTAMP(3));
```

## 本机复现

在已应用迁移的隔离库运行 `HMDP_SOCIAL_TEST_DSN='...' go test -race ./internal/social -run TestPublishChoosesModeFromCurrentFollowers -count=1`，可验证重复关注、跨过阈值、低于阈值，以及旧帖保留原模式。另设 `HMDP_SOCIAL_TEST_REDIS_ADDR` 并运行 `TestNotificationRecovery` 可复现通知恢复、失败分片重投和过期清理。

设置 `MYSQL_BIN_DIR`、`REDIS_BIN_DIR` 和 `KAFKA_BIN_DIR`。依次运行以下用例，每轮重新创建数据，使用 3 条热门作者帖子、1 条普通作者帖子和每秒 20 次通知读取。统计前 200 次读取的 P99、头尾粉丝可见时间、普通作者可见时间、Redis ZADD 次数、Kafka lag 及 Outbox 清空结果，并核对每篇帖子的投递模式。

```bash
SOCIAL_TEST_FOLLOWERS=100000 SOCIAL_PULL_MIN_FOLLOWERS=0 scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=100000 SOCIAL_PULL_MIN_FOLLOWERS=10000 scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=1000000 SOCIAL_PULL_MIN_FOLLOWERS=0 scripts/stage7-local.sh social
SOCIAL_TEST_FOLLOWERS=1000000 SOCIAL_PULL_MIN_FOLLOWERS=10000 scripts/stage7-local.sh social
```

## 3090 对照结果

以下为自动判定和通知读取修复之前的推拉对照。同一台 3090 上每轮启动一个 API、一个 Worker、MySQL 8.0.33、Redis 7.2.11 和 Kafka 4.1.2。Redis 启用 AOF `always`，Kafka 为单 Broker 三分区。每轮重新建立合成数据，以下时间从各自发帖请求开始计算。

| 热门作者粉丝 | 模式 | 第三篇到尾部样本 | 普通作者通知可见 | Redis ZADD | 首 200 次读 P99 |
| --- | --- | ---: | ---: | ---: | ---: |
| 100,000 | 推送 | 67.605 秒 | 67.588 秒 | 300,002 | 250.256 ms |
| 100,000 | 推拉结合 | 0.064 秒 | 1.757 秒 | 2 | 5.018 ms |
| 1,000,000 | 推送 | 115.866 秒 | 115.912 秒 | 3,000,002 | 15.770 ms |
| 1,000,000 | 推拉结合 | 0.064 秒 | 0.409 秒 | 2 | 4.724 ms |

推送组有数据的 Kafka 分区最终 lag 为 0，采样最大单分区 lag 为 2，部分早期采样的 offset 尚未提交。推拉组采到并在结束时核对的 lag 为 0。四轮均通过头尾样本、通知排序、去重、取关及 MySQL Outbox 归零检查。普通作者的两个关注者仍收到推送。每组运行一次，主机负载会波动。读流量来自三个轮询账号，每秒约 20 次，百万指关注关系数。

另做了一轮兼容性演练。先合并一条旧推送和一条读时投递，再向 Kafka 重放两次旧推送事件并启动 Worker。消费积压归零，通知仍各显示一条。

## 自动判定回归（迁移 017 之前）

在 3090 上以默认阈值 10,000、100 万条热门作者关注关系复跑。`EXPLAIN ANALYZE` 确认使用 `idx_followers` 的覆盖索引读取 10,000 条，取到阈值记录耗时 3.78 ms。热门作者的三篇新帖全部标记为拉模式，首尾样本均可见。普通作者的一篇帖子走推送，Redis 增加 2 次 `ZADD`，对应其两位粉丝。Kafka 已使用分区的 lag、MySQL Outbox 和待处理分发任务最终均为 0。

三个账号共轮询 201 次通知，前 200 次读取 P99 为 7.46 ms。百万是关注关系数，本轮没有模拟百万在线用户。

## 通知恢复和分片验收（迁移 017）

真实 MySQL 和 Redis 的回归覆盖 100 条已取关的 Redis 候选、通知键清空和单条丢失、通知 Redis 不可用、重复任务、8 次失败后人工重投、清理过期分片及过期 Kafka 事件的重放保护。4 个并行 Worker 分发 4,096 位粉丝并逐个校验 Redis 的写入结果。迁移复跑保留旧任务游标和完成去重记录。

强制推送三篇百万粉丝帖的首轮演练使用了 8 个 Worker 和共用 Redis，AOF 每次落盘。Kafka lag 已归零，三篇均拆成 8 个分片，通知页可以提前读到帖子。等待约 3 分钟后，24 个分片仍未清空。期间只完成了 38 次通知轮询，延迟最高约 600 毫秒。Kafka 消费完成与 Redis 分发完成是两个验收指标。此结果不能与上面的历史表直接比较。

第二轮对单篇百万粉丝帖强制推送，使用 4 个 Worker，测试 Redis 的 `appendfsync` 设为 `everysec`。热门帖拆成 8 个分片，普通帖使用 1 个分片。负载启动后 99 秒观察到所有分片完成，尾部粉丝的 Redis 通知已核对，失败分片和 Kafka lag 均为 0。Redis `ZADD` 共 1,000,006 次，含普通作者及读回填产生的写入。三个账号的 200 次通知读取 P99 为 18.924 ms。

```bash
SOCIAL_TEST_FOLLOWERS=1000000 SOCIAL_TEST_POPULAR_POSTS=1 SOCIAL_TEST_REDIS_AOF_FSYNC=everysec \
  SOCIAL_PULL_MIN_FOLLOWERS=0 SOCIAL_FANOUT_WORKERS=4 scripts/stage7-local.sh social
```
