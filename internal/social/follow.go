package social

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func (s *Service) changeFollow(ctx context.Context, viewer, target uint64, follow bool) error {
	if viewer == target {
		return errInvalidFollow
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`INSERT INTO social_follow_versions (user_id, revision) VALUES (?, 1)
			ON DUPLICATE KEY UPDATE revision = revision + 1`, viewer).Error; err != nil {
			return err
		}
		if !follow {
			return tx.Exec("DELETE FROM social_follows WHERE user_id = ? AND follow_user_id = ?", viewer, target).Error
		}
		var account profile
		if err := tx.Table("tb_user").Select("id").Take(&account, "id = ?", target).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errInvalidFollow
			}
			return err
		}
		return tx.Exec(`INSERT INTO social_follows (user_id, follow_user_id) VALUES (?, ?)
			ON DUPLICATE KEY UPDATE follow_user_id = follow_user_id`, viewer, target).Error
	})
}

var commonSets = redis.NewScript(`
if redis.call('SISMEMBER', KEYS[1], '0') == 0 or redis.call('SISMEMBER', KEYS[2], '0') == 0 then
    return nil
end
return redis.call('SINTER', KEYS[1], KEYS[2])
`)

const maxCachedFollows = 2000

func (s *Service) followSet(ctx context.Context, id uint64) (string, error) {
	var revision uint64
	if err := s.db.WithContext(ctx).Raw("SELECT COALESCE(MAX(revision), 0) FROM social_follow_versions WHERE user_id = ?", id).Scan(&revision).Error; err != nil {
		return "", err
	}
	key := fmt.Sprintf("social:{follows}:v1:%d:%d", id, revision)
	ready, err := s.cache.SIsMember(ctx, key, "0").Result()
	if err != nil || ready {
		return key, err
	}
	var ids []uint64
	if err := s.db.WithContext(ctx).Table("social_follows").Where("user_id = ?", id).Limit(maxCachedFollows+1).Pluck("follow_user_id", &ids).Error; err != nil {
		return "", err
	}
	if len(ids) > maxCachedFollows {
		return "", nil
	}
	members := make([]any, 1, 1+len(ids))
	members[0] = "0"
	for _, member := range ids {
		members = append(members, strconv.FormatUint(member, 10))
	}
	_, err = s.cache.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, key)
		pipe.SAdd(ctx, key, members...)
		pipe.Expire(ctx, key, 5*time.Minute)
		return nil
	})
	return key, err
}

func (s *Service) commonFollows(ctx context.Context, viewer, other uint64) ([]profile, error) {
	items := make([]profile, 0)
	query := s.db.WithContext(ctx).Table("tb_user AS u").Select("u.id, COALESCE(u.nick_name, '') AS nick_name, COALESCE(u.icon, '') AS icon")
	cacheCtx, cancel := context.WithTimeout(ctx, 350*time.Millisecond)
	defer cancel()
	first, firstErr := s.followSet(cacheCtx, viewer)
	if firstErr == nil && first != "" {
		second, err := s.followSet(cacheCtx, other)
		if err == nil && second != "" {
			members, err := commonSets.Run(cacheCtx, s.cache, []string{first, second}).StringSlice()
			if err == nil {
				ids := make([]uint64, 0, len(members))
				for _, member := range members {
					if id, err := strconv.ParseUint(member, 10, 64); err == nil && id != 0 {
						ids = append(ids, id)
					}
				}
				if len(ids) == 0 {
					return items, nil
				}
				err = query.Where("u.id IN ?", ids).Order("u.id").Find(&items).Error
				return items, err
			}
		}
	}
	err := query.Joins("JOIN social_follows a ON a.follow_user_id = u.id AND a.user_id = ?", viewer).
		Joins("JOIN social_follows b ON b.follow_user_id = u.id AND b.user_id = ?", other).Order("u.id").Find(&items).Error
	return items, err
}
