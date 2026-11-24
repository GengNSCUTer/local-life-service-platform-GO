# 爆帖、楼中楼与社交通知容量

爆帖同时产生详情读取、点赞和评论。旧写路径需要为每次互动独占 `tb_blog` 的一行，旧详情直接查 MySQL。新版可以按用户 ID 把互动计数分散到 16 行，同时用短缓存承接公共详情。大 V 通知用读时拉取，普通作者的通知仍走 Kafka 和 Redis。

## 回复与计数

`POST /blog/:id/comments` 的 `parentId` 可以指向同一篇帖子的任意未删除评论。顶层评论下按 ID 平铺。读取时，`parentId` 是用于分页的根评论，`replyToCommentId` 是实际被回复的评论。历史一级回复推导出原来的回复目标。已删顶层评论仍有回复时保留占位。已删的回复内容保持清空，已有后续回复仍在同一楼层可查。可选 `Idempotency-Key` 最长 128 字节，按博客和作者去重。重复请求返回同一个 `201` 和评论 ID，改变内容或回复目标时返回 `409`。

关系或评论与桶增量在同一事务提交。点赞和删评使用同一个用户所属桶，因此并发重试和取消沿用相同锁顺序。汇总 Worker 每秒尝试把增量加到旧展示列并清零待处理增量。更新展示列和清桶在一个事务里。历史点赞基数保留，区分个人最新操作与展示总数。指标 `hmdp_social_counter_projection_delay_seconds` 观察汇总延迟，详情缓存还可能保留上一个版本最多 3 秒。

## 详情、榜单、动态和通知

- 博客公共详情缓存 3 秒，Redis 租约合并跨进程回源。个人 `isLike` 每次独立查 MySQL。评论正文仍从 MySQL 读取。
- `GET /blog/hot?snapshot=true` 返回热门前 1,000 条的分页视图。快照每约 10 秒生成、保留 60 秒，游标固定榜单版本。过期返回 `410`，客户端刷新首页。原热门页码和游标继续可用。
- `SOCIAL_BOUNDED_FEED=true` 启用候选查询，每个关注作者通过索引提供最多 11 条。全局从中取最新项，批量查询显示信息。保留 `cursor`、`lastId` 和旧响应字段。
- `SOCIAL_FANOUT_WORKERS=2` 时，Worker 先把 Kafka 事件幂等入 MySQL，再提交 offset。单次领取一条任务的 512 人批次，Redis 更新后保存游标，再把任务放回队尾。每个租约 10 秒。宕机后可以重放一批，已结束任务保留其唯一记录用来挡重放。通知按当前关注关系过滤。
- `SOCIAL_WRITE_CONCURRENCY` 每个 API 默认 12，社交写按本地并发预算拒绝过载。`MEDIA_UPLOAD_CONCURRENCY` 默认 2，单独限制图片读入、解码和上传。超载响应 `503` 和 `Retry-After`。

## 部署和回退

应用 `014` 和 `015` 后升级 API 与 Worker。执行期间暂停社交写入，旧展示列直接作为基数，待汇总表从空表开始。先完成所有进程升级再恢复写入。回退前暂停写入和通知生产，等待 `social_blog_counter_buckets` 中的脏标记与 `social_fanout_tasks` 待处理记录归零，再切换模式或程序。已产生的多级回复保留平铺根 ID，旧代码可以读该楼层。修改选项时保持所有 API 一致。

## 本机复现

设置 `MYSQL_BIN_DIR`、`REDIS_BIN_DIR` 和 `KAFKA_BIN_DIR`。`social-hot` 默认造 100 万篇帖、爆帖 10 万条评论、其中一楼 1 万条回复，90% 的请求命中该帖。读、写、混合和 Feed 可通过 `SOCIAL_HOT_WORKLOAD=read|write|mixed|feed` 分开跑。`SOCIAL_HOT_RATE` 为每秒计划操作数，发评加删评计为一组操作。脚本记录调度丢弃、响应分布和 P99。

```bash
SOCIAL_COUNTER_BUCKETS=false SOCIAL_DETAIL_CACHE=false scripts/stage7-local.sh social-hot
SOCIAL_COUNTER_BUCKETS=true SOCIAL_DETAIL_CACHE=false scripts/stage7-local.sh social-hot
SOCIAL_COUNTER_BUCKETS=false SOCIAL_DETAIL_CACHE=true scripts/stage7-local.sh social-hot
SOCIAL_COUNTER_BUCKETS=true SOCIAL_DETAIL_CACHE=true scripts/stage7-local.sh social-hot
SOCIAL_HOT_SECONDS=3600 SOCIAL_HOT_RATE=200 SOCIAL_HOT_ORDER_RATE=2 scripts/stage7-local.sh social-hot
```
