package social

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

const blogTopic = "hmdp.social.blog-published.v1"
const notificationPull = 1
const notificationLimit = 100
const notificationRetention = 30 * 24 * time.Hour

type publishedBlog struct {
	BlogID    uint64    `json:"blogId"`
	AuthorID  uint64    `json:"authorId"`
	CreatedAt time.Time `json:"createdAt"`
}

func (s *Service) RunWorkers(ctx context.Context, brokers []string, client *kafka.Client, transport *kafka.Transport, started func()) error {
	local, stop := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { stop(); workers.Wait() }()
	workers.Go(func() { s.counterWorker(local) })
	workers.Go(func() { s.rankWorker(local) })
	workers.Go(func() { s.pruneFanout(local) })
	for range s.fanoutWorkers {
		workers.Go(func() { s.fanoutWorker(local) })
	}
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, 3*time.Second)
		result, err := client.CreateTopics(operation, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
			{Topic: blogTopic, NumPartitions: 3, ReplicationFactor: -1},
		}})
		cancel()
		if err == nil {
			err = result.Errors[blogTopic]
		}
		if err == nil || errors.Is(err, kafka.TopicAlreadyExists) {
			break
		}
		s.logger.Warn("create social Kafka topic", "error", err)
		if !wait(ctx, 2*time.Second) {
			return nil
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Transport: transport, Topic: blogTopic,
		RequiredAcks: kafka.RequireAll, Balancer: &kafka.Hash{}, BatchTimeout: 10 * time.Millisecond,
		ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, MaxAttempts: 3}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: blogTopic, GroupID: "hmdp-social-notifications-v1", StartOffset: kafka.FirstOffset, MaxWait: 500 * time.Millisecond})
	workers.Go(func() { s.relay(ctx, writer) })
	workers.Go(func() { s.consume(ctx, reader) })
	started()
	workers.Wait()
	return errors.Join(reader.Close(), writer.Close())
}

func (s *Service) relay(ctx context.Context, writer *kafka.Writer) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.publishNext(operation, writer)
		cancel()
		if err == nil {
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) && ctx.Err() == nil {
			s.logger.Warn("retry social outbox", "error", err)
		}
		wait(ctx, time.Second)
	}
}

func (s *Service) publishNext(ctx context.Context, writer *kafka.Writer) error {
	var event publishedBlog
	if err := s.db.WithContext(ctx).Table("social_blog_outbox").Order("blog_id").Take(&event).Error; err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(strconv.FormatUint(event.AuthorID, 10)), Value: payload}); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Exec("DELETE FROM social_blog_outbox WHERE blog_id = ?", event.BlogID).Error
}

func (s *Service) consume(ctx context.Context, reader *kafka.Reader) {
	for ctx.Err() == nil {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warn("read social Kafka event", "error", err)
			}
			wait(ctx, time.Second)
			continue
		}
		var event publishedBlog
		if err := json.Unmarshal(message.Value, &event); err != nil || event.AuthorID == 0 || event.BlogID == 0 || event.CreatedAt.IsZero() {
			s.logger.Warn("invalid social Kafka event", "partition", message.Partition, "offset", message.Offset)
		} else {
			for ctx.Err() == nil {
				operation, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := s.enqueueFanout(operation, event)
				cancel()
				if err == nil {
					break
				}
				s.logger.Warn("retry social notification event", "blog_id", event.BlogID, "error", err)
				wait(ctx, time.Second)
			}
		}
		for ctx.Err() == nil {
			operation, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := reader.CommitMessages(operation, message)
			cancel()
			if err == nil {
				break
			}
			s.logger.Warn("retry social Kafka offset", "error", err)
			wait(ctx, time.Second)
		}
	}
}

func notificationKey(userID uint64) string {
	return "social:notifications:" + strconv.FormatUint(userID, 10)
}

// Apply each follower's update atomically so a partial pipeline can be retried.
var storeNotification = redis.NewScript(`
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -101)
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
`)

func (s *Service) notifyBatch(ctx context.Context, event publishedBlog, last, end uint64) ([]uint64, error) {
	var followers []uint64
	err := s.db.WithContext(ctx).Table("social_follows").Where("follow_user_id = ? AND user_id > ? AND user_id <= ?", event.AuthorID, last, end).
		Order("user_id").Limit(fanoutBatchSize).Pluck("user_id", &followers).Error
	if err != nil || len(followers) == 0 {
		return followers, err
	}
	blogID := strconv.FormatUint(event.BlogID, 10)
	_, err = s.notificationCache.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, id := range followers {
			storeNotification.Eval(ctx, pipe, []string{notificationKey(id)}, event.CreatedAt.UnixMilli(), blogID, int(notificationRetention/time.Second))
		}
		return nil
	})
	return followers, err
}

// Restore eligible push entries without displacing fresher entries written by workers.
var repairNotifications = redis.NewScript(`
local count = redis.call('ZCARD', KEYS[1])
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local inserted = 0
for i = 2, #ARGV, 2 do
    local score = tonumber(ARGV[i])
    local cutoff = tonumber(oldest[2])
    if count < 100 or score > cutoff or (score == cutoff and ARGV[i+1] > oldest[1]) then
        if not redis.call('ZSCORE', KEYS[1], ARGV[i+1]) then
            inserted = inserted + redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i+1])
        end
    end
end
if inserted > 0 then
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -101)
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return inserted
`)

func (s *Service) loadNotifications(ctx context.Context, viewer uint64) ([]blogView, error) {
	items := make([]blogView, 0)
	// Bound each author's contribution before joining blog bodies and merging the page.
	head := s.db.WithContext(ctx).Raw(`SELECT recent.id, recent.create_time FROM social_follows AS f
		JOIN LATERAL (SELECT b.id, b.create_time FROM tb_blog b FORCE INDEX (idx_blog_author_recent)
		WHERE b.user_id = f.follow_user_id AND b.create_time >= TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP)
		ORDER BY b.create_time DESC, b.id DESC LIMIT ?) AS recent ON TRUE
		WHERE f.user_id = ? ORDER BY recent.create_time DESC, recent.id DESC LIMIT ?`, -int(notificationRetention/time.Second), notificationLimit, viewer, notificationLimit)
	err := s.blogs(ctx, viewer).
		Select(blogViewColumns+", b.notification_mode").Joins("JOIN (?) AS head ON head.id = b.id", head).
		Order("head.create_time DESC, head.id DESC").Find(&items).Error
	return items, err
}

func (s *Service) notifications(c *gin.Context) {
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	items, err := s.loadNotifications(ctx, viewer)
	if err == nil {
		s.repairNotificationCache(ctx, viewer, items)
	}
	s.respond(c, items, err)
}

func (s *Service) repairNotificationCache(ctx context.Context, viewer uint64, items []blogView) {
	args := make([]any, 1, 1+2*len(items))
	args[0] = int(notificationRetention / time.Second)
	for _, item := range items {
		if item.NotificationMode == 0 {
			args = append(args, item.CreatedAt.UnixMilli(), strconv.FormatUint(item.ID, 10))
		}
	}
	if len(args) == 1 {
		return
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	count, err := repairNotifications.Run(cacheCtx, s.notificationCache, []string{notificationKey(viewer)}, args...).Int()
	if s.metrics != nil {
		if err != nil {
			s.metrics.notificationCache.WithLabelValues("unavailable").Inc()
		} else if count > 0 {
			s.metrics.notificationCache.WithLabelValues("repaired").Inc()
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
