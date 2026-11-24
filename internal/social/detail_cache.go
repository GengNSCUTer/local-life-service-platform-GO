package social

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var finishBlogFill = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if ARGV[2] ~= '' then redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3]) end
return redis.call('DEL', KEYS[1])`)

func (s *Service) loadDetail(ctx context.Context, id, viewer uint64) (blogView, error) {
	var item blogView
	if !s.detailCache {
		err := s.blogs(ctx, viewer).Take(&item, "b.id = ?", id).Error
		return item, err
	}
	item, err := s.publicDetail(ctx, id)
	if err != nil || viewer == 0 {
		return item, err
	}
	var liked int64
	err = s.db.WithContext(ctx).Table("social_blog_likes").Where("blog_id = ? AND user_id = ?", id, viewer).Count(&liked).Error
	item.IsLike = liked != 0
	return item, err
}

func (s *Service) publicDetail(ctx context.Context, id uint64) (blogView, error) {
	var item blogView
	prefix := "social:blog:{" + strconv.FormatUint(id, 10) + "}:"
	key, lease := prefix+"detail", prefix+"fill"
	token := rand.Text()
	for ctx.Err() == nil {
		lookup, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
		value, err := s.cache.Get(lookup, key).Bytes()
		if err == nil && string(value) == "null" {
			s.cacheMetric("negative_hit")
			cancel()
			return item, gorm.ErrRecordNotFound
		}
		if err == nil && json.Unmarshal(value, &item) == nil && item.ID == id {
			s.cacheMetric("hit")
			cancel()
			return item, nil
		}
		if err != nil && !errors.Is(err, redis.Nil) {
			cancel()
			return s.queryPublicDetail(ctx, id)
		}
		locked, err := s.cache.SetNX(lookup, lease, token, time.Second).Result()
		cancel()
		if err != nil {
			return s.queryPublicDetail(ctx, id)
		}
		if locked {
			// Recheck after acquiring the lease in case the previous owner just filled it.
			lookup, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
			cached, _ := s.cache.Get(lookup, key).Bytes()
			cancel()
			var payload []byte
			var loadErr error
			switch {
			case string(cached) == "null":
				loadErr = gorm.ErrRecordNotFound
			case len(cached) > 0 && json.Unmarshal(cached, &item) == nil && item.ID == id:
			default:
				item, loadErr = s.queryPublicDetail(ctx, id)
				if errors.Is(loadErr, gorm.ErrRecordNotFound) {
					payload = []byte("null")
				} else if loadErr == nil {
					payload, _ = json.Marshal(item)
				}
			}
			finish, cancel := context.WithTimeout(context.WithoutCancel(ctx), 75*time.Millisecond)
			_ = finishBlogFill.Run(finish, s.cache, []string{lease, key}, token, string(payload), 3000).Err()
			cancel()
			return item, loadErr
		}
		if !wait(ctx, 20*time.Millisecond) {
			break
		}
	}
	return item, ctx.Err()
}

func (s *Service) queryPublicDetail(ctx context.Context, id uint64) (blogView, error) {
	var item blogView
	operation, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
	defer cancel()
	select {
	case s.detailReads <- struct{}{}:
		defer func() { <-s.detailReads }()
	case <-operation.Done():
		return item, operation.Err()
	}
	s.cacheMetric("mysql")
	err := s.blogs(operation, 0).Take(&item, "b.id = ?", id).Error
	if err == nil {
		item.IsLike = false
	}
	return item, err
}
