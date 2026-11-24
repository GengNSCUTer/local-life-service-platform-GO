package social

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const counterBuckets = 16

// Lock the blog before its buckets in both writers and the projection worker.
func (s *Service) lockBlog(tx *gorm.DB, blogID uint64) error {
	strength := "UPDATE"
	if s.bucketCounters {
		strength = "SHARE"
	}
	var row struct{ ID uint64 }
	return tx.Table("tb_blog").Clauses(clause.Locking{Strength: strength}).Select("id").Take(&row, "id = ?", blogID).Error
}

func lockCounter(tx *gorm.DB, blogID, userID uint64) error {
	return tx.Exec(`INSERT INTO social_blog_counter_buckets (blog_id, bucket) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE bucket = social_blog_counter_buckets.bucket`, blogID, userID%counterBuckets).Error
}

func (s *Service) changeCounters(tx *gorm.DB, blogID, userID uint64, likes, comments int) error {
	if !s.bucketCounters {
		return tx.Exec(`UPDATE tb_blog SET liked = IF(? = 0, liked, GREATEST(CAST(COALESCE(liked, 0) AS SIGNED) + ?, 0)),
			comments = IF(? = 0, comments, GREATEST(CAST(COALESCE(comments, 0) AS SIGNED) + ?, 0)) WHERE id = ?`,
			likes, likes, comments, comments, blogID).Error
	}
	return tx.Exec(`INSERT INTO social_blog_counter_buckets (blog_id, bucket, liked_delta, comments_delta, dirty_since)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(3)) AS increment
		ON DUPLICATE KEY UPDATE liked_delta = social_blog_counter_buckets.liked_delta + increment.liked_delta,
		comments_delta = social_blog_counter_buckets.comments_delta + increment.comments_delta,
		dirty_since = COALESCE(social_blog_counter_buckets.dirty_since, increment.dirty_since)`,
		blogID, userID%counterBuckets, likes, comments).Error
}

func (s *Service) projectCounters(ctx context.Context) error {
	var ids []uint64
	if err := s.db.WithContext(ctx).Raw(`SELECT blog_id FROM social_blog_counter_buckets
		WHERE dirty_since IS NOT NULL GROUP BY blog_id ORDER BY MIN(dirty_since), blog_id LIMIT 128`).Scan(&ids).Error; err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.projectBlog(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) projectBlog(ctx context.Context, id uint64) error {
	var age float64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row struct{ ID uint64 }
		err := tx.Table("tb_blog").Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").Take(&row, "id = ?", id).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var delta struct {
			Likes, Comments int64
			AgeMS           int64
		}
		if err := tx.Raw(`SELECT COALESCE(SUM(liked_delta), 0) AS likes, COALESCE(SUM(comments_delta), 0) AS comments,
			COALESCE(TIMESTAMPDIFF(MICROSECOND, MIN(dirty_since), CURRENT_TIMESTAMP(3)) DIV 1000, 0) AS age_ms
			FROM social_blog_counter_buckets WHERE blog_id = ? AND dirty_since IS NOT NULL`, id).Scan(&delta).Error; err != nil {
			return err
		}
		age = float64(delta.AgeMS) / 1000
		if delta.Likes != 0 || delta.Comments != 0 {
			if err := tx.Exec(`UPDATE tb_blog SET liked = IF(? = 0, liked, GREATEST(CAST(COALESCE(liked, 0) AS SIGNED) + ?, 0)),
				comments = IF(? = 0, comments, GREATEST(CAST(COALESCE(comments, 0) AS SIGNED) + ?, 0)) WHERE id = ?`,
				delta.Likes, delta.Likes, delta.Comments, delta.Comments, id).Error; err != nil {
				return err
			}
		}
		return tx.Exec(`UPDATE social_blog_counter_buckets SET liked_delta = 0, comments_delta = 0, dirty_since = NULL
			WHERE blog_id = ? AND dirty_since IS NOT NULL`, id).Error
	})
	if err == nil && age > 0 && s.metrics != nil {
		s.metrics.counterLag.Observe(age)
	}
	return err
}

func (s *Service) counterWorker(ctx context.Context) {
	for wait(ctx, time.Second) {
		operation, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
		err := s.projectCounters(operation)
		cancel()
		if err != nil && ctx.Err() == nil {
			s.logger.Warn("project social counters", "error", err)
		}
	}
}
