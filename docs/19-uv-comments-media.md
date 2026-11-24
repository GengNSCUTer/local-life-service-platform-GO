# 店铺 UV、博客评论和图片

## 每日店铺详情 UV

`GET /shop/:id` 成功时执行一次短超时的 Redis Lua，把用户或浏览器身份加入北京时间当天的 HyperLogLog。`GET /shop/statistics/uv?date=2026-11-24` 用 `SHOP_ADMIN_USER_ID` 配置的账号查询，返回 `date`、`uv` 和 `approximate`。Redis 键从统计日开始保留 30 天。统计只覆盖店铺详情，重复打开不同店铺也只算一个身份。登录和匿名身份分别去重，清除 Cookie、跨浏览器或先匿名后登录会影响口径。

源码从 `shop.get` 到 `recordVisit`。`user.VisitorLogin` 允许匿名访问，并给有效登录者附上身份。`context.WithTimeout` 限制统计使用的时间。

## 评论

已有库先应用 `011_blog_comments.sql`。

- `POST /blog/:id/comments` 需要登录，传 `{"content":"好吃"}`，加 `parentId` 可回复同一篇博客的一条顶层评论。
- `GET /blog/:id/comments` 公开读取。`afterId` 是上一页的 `nextAfterId`，每页 20 条。传 `parentId` 单独读取一级回复。顶层评论被删除且仍有回复时，保留内容已清空的占位。
- `DELETE /blog/:id/comments/:commentId` 仅评论作者可执行，重复删除返回成功。写入和删除都在同一个 MySQL 事务中更新博客评论数。

## 图片

本地执行 `docker compose -f compose.yaml -f compose.media.yaml up -d minio`，登录 `127.0.0.1:29001` 创建 `hmdp-images` 桶，并为演示桶授予匿名读取。用 `.env.example` 的 `MEDIA_*` 设置指向 MinIO，外部存储填写对应的桶端点、访问凭证和公开读取 URL。应用账号只授予目标桶所需权限。未配置存储时上传接口返回 `503`。

```bash
curl -f -H "Authorization: Bearer $TOKEN" -F image=@meal.jpg http://127.0.0.1:8080/media/images
```

接口限制单图 5 MiB 和 1600 万像素，按解码结果只接受 JPEG 和 PNG。响应的 `data.url` 可以放进发帖请求的 `images`。部署时使用专门的图片读取域名，桶只授予公开读取权限，写操作通过 API 登录校验。

## 本机验收

在隔离 MySQL 8.0.33、Redis 7.2.11 和 MinIO `RELEASE.2025-04-22T22-12-26Z` 上，用匿名浏览器和登录用户重复访问，日 UV 为 2。管理员可读，普通账号得到 `403`。24 条并发评论落库后评论数准确，分页、回帖层级、越权和重复删除通过。真实上传 PNG 后能匿名下载同一对象，发帖后能读回图片地址，超限、伪装和截断文件均被拒绝。

额外加入 10,000 个独立测试身份，精确值为 10,002，HyperLogLog 返回 10,006。前一天的 Key 独立，当前 Key 的 TTL 落在 29 至 30 天之间。注入错误类型的统计 Key 后，店铺详情仍返回成功。

复测时，身份读取超时会跳过该次 UV，避免把已登录请求误计成匿名。停止 MinIO 后重新上传正常图片，API 返回 `503`。
