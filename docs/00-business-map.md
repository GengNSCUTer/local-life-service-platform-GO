# 业务地图

## 1. 项目定位

项目是一个本地生活服务平台，核心业务围绕商户、优惠券、探店内容和用户关系展开。Redis 不是简单的缓存组件，而是分别承担登录态、热点查询、集合运算、排行榜、地理位置、位图统计和异步订单入口等职责。

Java 基线项目已经实现了课程中的主要业务能力。评论 Controller 和 Service 仍然是空壳，图片上传使用本地文件目录。Go 版本先围绕已经实现且能体现并发与中间件设计的业务推进。

## 2. 业务模块

| 领域 | 业务能力 | MySQL | Redis | 异步 |
| --- | --- | --- | --- | --- |
| 用户 | 验证码登录、Token 会话、用户资料、签到 | `tb_user`、`tb_user_info`、`tb_sign` | String、Hash、Bitmap | 否 |
| 商户 | 类型、详情、名称查询、缓存、附近商户 | `tb_shop`、`tb_shop_type` | String、GEO、互斥锁 | 否 |
| 优惠券 | 普通券、秒杀券、店铺券列表 | `tb_voucher`、`tb_seckill_voucher` | String、计数器 | 否 |
| 秒杀订单 | 抢购、库存预占、一人一单、订单落库 | `seckill_activities`、`seckill_orders` | Lua、Hash、String、Sorted Set | Kafka |
| 探店内容 | 发布、热门、个人内容、详情 | `tb_blog` | Sorted Set、Feed | 否 |
| 点赞 | 点赞、取消点赞、点赞排行 | `tb_blog` | Set 或 Sorted Set | 否 |
| 关注 | 关注、取关、共同关注 | `tb_follow` | Set | 否 |
| Feed | 向粉丝推送、滚动分页 | `tb_blog` | Sorted Set | 后续可异步 |
| 评论 | 评论和回复 | `tb_blog_comments` | 后续决定 | 后续决定 |
| 文件 | 图片上传和删除 | 文件系统或对象存储 | 无 | 否 |
| 统计 | UV 去重 | 无专用表 | HyperLogLog | 否 |

## 3. 主要业务链路

### 登录

1. 客户端提交手机号。
2. API 校验手机号并写入验证码 Key。
3. 客户端提交手机号和验证码。
4. 服务使用 Redis Lua 原子校验并删除验证码。
5. 查询或创建用户。
6. 生成随机 Token，将安全用户字段写入 Redis Hash。
7. 后续请求由 Gin 鉴权中间件读取 Token，并把当前用户放入请求 Context。
8. 有效请求刷新 Token 过期时间。

Go 版本不使用 Java ThreadLocal。请求范围的数据放在 `context.Context` 或 Gin Context 中，后台任务使用显式参数传递用户和请求信息。

### 商户查询

1. 先读 `cache:shop:{id}`。
2. 命中正常数据则直接返回。
3. 命中空值则直接返回不存在。
4. 未命中时竞争 `lock:shop:{id}`，胜者回源 MySQL 并重建缓存。
5. 数据库不存在时写入短 TTL 空值。
6. 数据库更新成功后失效缓存，避免旧缓存长期覆盖新数据。

第一版实现缓存空值和互斥重建。逻辑过期方案作为第二个实验，用于比较响应延迟和后台重建复杂度。

### 秒杀

1. API 校验用户身份和请求参数。
2. Redis Lua 同时检查活动库存、用户是否重复购买、扣减 Redis 库存并写入订单事件。
3. API 返回 `202` 和订单 ID。后台从 Redis 待投递队列发布到 Kafka。
4. Kafka Consumer Group 消费订单事件。
5. Consumer 在 MySQL 事务中条件扣减数据库库存并插入订单。
6. 数据库唯一约束和消费幂等共同保证一人一单。
7. 处理成功后提交 Kafka offset。
8. 失败消息按重试策略处理，超过次数进入死信主题。
9. 对账任务根据 MySQL 事实决定确认、补偿或人工处理。

### 关注和 Feed

关注关系最终存储在 MySQL，Redis Set 用于快速判断关注状态和计算共同关注。发布笔记时向粉丝收件箱写入笔记 ID，收件箱使用 Sorted Set 保存发布时间。读取 Feed 使用 `maxTime + offset` 滚动游标，避免普通页码分页在新内容插入时重复或遗漏。

### 位置和统计

- GEO 按商户类型保存经纬度，查询距离后再回表读取完整商户信息。
- Bitmap 按用户和月份保存签到位，日期映射为位偏移。
- HyperLogLog 用于允许估算误差的 UV 统计。
