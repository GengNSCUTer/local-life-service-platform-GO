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

type publishedBlog struct {
	BlogID    uint64    `json:"blogId"`
	AuthorID  uint64    `json:"authorId"`
	CreatedAt time.Time `json:"createdAt"`
}

func (s *Service) RunWorkers(ctx context.Context, brokers []string, client *kafka.Client, transport *kafka.Transport, started func()) error {
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
	var workers sync.WaitGroup
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
				err := s.notifyFollowers(ctx, event)
				if err == nil {
					break
				}
				s.logger.Warn("retry blog notifications", "blog_id", event.BlogID, "error", err)
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

func (s *Service) notifyFollowers(ctx context.Context, event publishedBlog) error {
	const batchSize = 128
	var last uint64
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, 5*time.Second)
		var followers []uint64
		err := s.db.WithContext(operation).Table("social_follows").Where("follow_user_id = ? AND user_id > ?", event.AuthorID, last).
			Order("user_id").Limit(batchSize).Pluck("user_id", &followers).Error
		if err == nil && len(followers) != 0 {
			_, err = s.cache.Pipelined(operation, func(pipe redis.Pipeliner) error {
				for _, id := range followers {
					key := notificationKey(id)
					pipe.ZAdd(operation, key, redis.Z{Score: float64(event.CreatedAt.UnixMilli()), Member: strconv.FormatUint(event.BlogID, 10)})
					pipe.ZRemRangeByRank(operation, key, 0, -101)
					pipe.Expire(operation, key, 30*24*time.Hour)
				}
				return nil
			})
		}
		cancel()
		if err != nil || len(followers) < batchSize {
			return err
		}
		last = followers[len(followers)-1]
	}
	return ctx.Err()
}

func (s *Service) notifications(c *gin.Context) {
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	members, err := s.cache.ZRevRange(ctx, notificationKey(viewer), 0, 99).Result()
	if err != nil {
		s.respond(c, nil, err)
		return
	}
	ids := make([]uint64, 0, len(members))
	for _, member := range members {
		if id, err := strconv.ParseUint(member, 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	items := make([]blogView, 0)
	if len(ids) > 0 {
		var rows []blogView
		err = s.blogs(ctx, viewer).Joins("JOIN social_follows f ON f.follow_user_id = b.user_id AND f.user_id = ?", viewer).
			Where("b.id IN ?", ids).Find(&rows).Error
		byID := make(map[uint64]blogView, len(rows))
		for _, row := range rows {
			byID[row.ID] = row
		}
		for _, id := range ids {
			if row, ok := byID[id]; ok {
				items = append(items, row)
			}
		}
	}
	s.respond(c, items, err)
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
