package social

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	fanoutBatchSize   = 512
	fanoutRangeCount  = 8
	fanoutMaxAttempts = 8
)

type fanoutShard struct {
	BlogID       uint64
	Shard        uint16
	LastFollower uint64
	EndFollower  uint64
	Attempts     uint8
}

func (s *Service) enqueueFanout(ctx context.Context, event publishedBlog) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Exec(`INSERT INTO social_fanout_jobs (blog_id, author_id, created_at)
			SELECT id, user_id, create_time FROM tb_blog
			WHERE id = ? AND user_id = ? AND notification_mode = 0
			AND create_time >= TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP)
			ON DUPLICATE KEY UPDATE blog_id = social_fanout_jobs.blog_id`,
			event.BlogID, event.AuthorID, -int(notificationRetention/time.Second))
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		// In MySQL's insertion connection, a duplicate no-op reports zero affected rows.
		var bounds struct{ FirstFollower, LastFollower, SplitAt uint64 }
		followers := ` FROM social_follows FORCE INDEX (idx_followers) WHERE follow_user_id = ? ORDER BY user_id `
		if err := tx.Raw(`SELECT COALESCE((SELECT user_id`+followers+`ASC LIMIT 1), 0) AS first_follower,
			COALESCE((SELECT user_id`+followers+`DESC LIMIT 1), 0) AS last_follower,
			COALESCE((SELECT user_id`+followers+`ASC LIMIT 1 OFFSET ?), 0) AS split_at`,
			event.AuthorID, event.AuthorID, event.AuthorID, fanoutBatchSize*fanoutRangeCount-1).Scan(&bounds).Error; err != nil {
			return err
		}
		if bounds.FirstFollower == 0 {
			return nil
		}
		count := 1
		if bounds.SplitAt != 0 {
			count = fanoutRangeCount
		}
		start := bounds.FirstFollower - 1
		width := (bounds.LastFollower - start) / uint64(count)
		shards := make([]map[string]any, 0, count)
		for i := range count {
			end := start + width
			if i == count-1 {
				end = bounds.LastFollower
			}
			shards = append(shards, map[string]any{"blog_id": event.BlogID, "shard": i, "last_follower": start, "end_follower": end})
			start = end
		}
		return tx.Table("social_fanout_shards").Create(shards).Error
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
}

func (s *Service) fanoutStep(ctx context.Context) (outcome string, err error) {
	var task fanoutShard
	token := rand.Text()
	db := s.db.WithContext(ctx)
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Table("social_fanout_shards").Where("due_at <= CURRENT_TIMESTAMP(3)").Order("due_at, blog_id, shard").
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&task).Error; err != nil {
			return err
		}
		return tx.Exec(`UPDATE social_fanout_shards SET lease_token = ?, due_at = TIMESTAMPADD(SECOND, 10, CURRENT_TIMESTAMP(3)),
			attempts = LEAST(attempts + 1, ?) WHERE blog_id = ? AND shard = ?`, token, fanoutMaxAttempts, task.BlogID, task.Shard).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return "", err
	}
	selection := db.Table("social_fanout_shards").Where("blog_id = ? AND shard = ? AND lease_token = ?", task.BlogID, task.Shard, token)
	updates := map[string]any{"due_at": nil, "lease_token": nil}
	var batchErr error
	if task.Attempts < fanoutMaxAttempts {
		operation, cancel := context.WithTimeout(ctx, 3*time.Second)
		var event publishedBlog
		batchErr = s.db.WithContext(operation).Table("social_fanout_jobs").Take(&event, "blog_id = ?", task.BlogID).Error
		var followers []uint64
		if batchErr == nil && event.CreatedAt.After(time.Now().Add(-notificationRetention)) {
			followers, batchErr = s.notifyBatch(operation, event, task.LastFollower, task.EndFollower)
		}
		cancel()
		if batchErr == nil || errors.Is(batchErr, gorm.ErrRecordNotFound) {
			updates["attempts"] = 0
			if len(followers) == fanoutBatchSize && followers[len(followers)-1] < task.EndFollower {
				updates["last_follower"] = followers[len(followers)-1]
				updates["due_at"] = gorm.Expr("CURRENT_TIMESTAMP(3)")
			}
			return "success", selection.Updates(updates).Error
		}
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if task.Attempts+1 >= fanoutMaxAttempts {
		outcome = "failed"
		updates["failed_at"] = gorm.Expr("CURRENT_TIMESTAMP(3)")
	} else {
		outcome = "retry"
		delay := 1 << min(task.Attempts, 6)
		updates["due_at"] = gorm.Expr("TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3))", delay)
	}
	result := selection.Updates(updates)
	if result.Error == nil && result.RowsAffected == 1 {
		s.logger.Warn("social fanout batch did not complete", "blog_id", task.BlogID, "shard", task.Shard,
			"attempts", min(task.Attempts+1, fanoutMaxAttempts), "outcome", outcome, "error", batchErr)
	}
	return outcome, result.Error
}

func (s *Service) fanoutWorker(ctx context.Context) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, 5*time.Second)
		outcome, err := s.fanoutStep(operation)
		cancel()
		if err == nil {
			if s.metrics != nil {
				s.metrics.fanout.WithLabelValues(outcome).Inc()
			}
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) && ctx.Err() == nil {
			s.logger.Warn("process social fanout batch", "error", err)
		}
		if !wait(ctx, 100*time.Millisecond) {
			return
		}
	}
}

func (s *Service) pruneFanout(ctx context.Context) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, 2*time.Second)
		result := s.db.WithContext(operation).Exec(`DELETE FROM social_fanout_jobs
			WHERE created_at < TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)) ORDER BY created_at, blog_id LIMIT 200`,
			-int((notificationRetention+7*24*time.Hour)/time.Second))
		cancel()
		if result.Error != nil && ctx.Err() == nil {
			s.logger.Warn("prune social fanout jobs", "error", result.Error)
		}
		delay := time.Minute
		if result.RowsAffected == 200 {
			delay = time.Second
		}
		if !wait(ctx, delay) {
			return
		}
	}
}
