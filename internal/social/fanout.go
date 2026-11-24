package social

import (
	"context"
	"crypto/rand"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const fanoutBatchSize = 512

type fanoutTask struct {
	BlogID       uint64
	AuthorID     uint64
	CreatedAt    time.Time
	LastFollower uint64
}

func (s *Service) enqueueFanout(ctx context.Context, event publishedBlog) error {
	return s.db.WithContext(ctx).Exec(`INSERT INTO social_fanout_tasks (blog_id, author_id, created_at)
		VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE blog_id = social_fanout_tasks.blog_id`, event.BlogID, event.AuthorID, event.CreatedAt.UTC()).Error
}

func (s *Service) fanoutStep(ctx context.Context) error {
	var task fanoutTask
	token := rand.Text()
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Table("social_fanout_tasks").Where("due_at <= CURRENT_TIMESTAMP(3)").Order("due_at, blog_id").
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&task).Error; err != nil {
			return err
		}
		return tx.Exec(`UPDATE social_fanout_tasks SET lease_token = ?, due_at = TIMESTAMPADD(SECOND, 10, CURRENT_TIMESTAMP(3)) WHERE blog_id = ?`, token, task.BlogID).Error
	})
	if err != nil {
		return err
	}
	event := publishedBlog{BlogID: task.BlogID, AuthorID: task.AuthorID, CreatedAt: task.CreatedAt}
	followers, err := s.notifyBatch(ctx, event, task.LastFollower)
	if err != nil {
		return err
	}
	if len(followers) < fanoutBatchSize {
		return s.db.WithContext(ctx).Exec(`UPDATE social_fanout_tasks SET due_at = NULL, lease_token = NULL WHERE blog_id = ? AND lease_token = ?`, task.BlogID, token).Error
	}
	return s.db.WithContext(ctx).Exec(`UPDATE social_fanout_tasks SET last_follower = ?, due_at = CURRENT_TIMESTAMP(3), lease_token = NULL
		WHERE blog_id = ? AND lease_token = ?`, followers[len(followers)-1], task.BlogID, token).Error
}

func (s *Service) fanoutWorker(ctx context.Context) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.fanoutStep(operation)
		cancel()
		if err == nil {
			if s.metrics != nil {
				s.metrics.fanout.WithLabelValues("success").Inc()
			}
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) && ctx.Err() == nil {
			if s.metrics != nil {
				s.metrics.fanout.WithLabelValues("retry").Inc()
			}
			s.logger.Warn("retry social fanout batch", "error", err)
		}
		if !wait(ctx, 100*time.Millisecond) {
			return
		}
	}
}
