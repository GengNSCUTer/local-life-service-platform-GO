package seckill

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const shardCount = 16

type redisShard uint8

func activityShard(id uint64) redisShard { return redisShard(id%shardCount + 1) }

func (shard redisShard) prefix() string {
	if shard == 0 {
		return keyPrefix
	}
	return fmt.Sprintf("seckill:{orders:v2:%02d}:", shard)
}

func (shard redisShard) activity(id uint64) string {
	return shard.prefix() + "activity:" + strconv.FormatUint(id, 10)
}

func (shard redisShard) outbox() string   { return shard.prefix() + "outbox" }
func (shard redisShard) timeouts() string { return shard.prefix() + "timeouts" }

func (shard redisShard) newOrderID() string {
	if shard == 0 {
		return rand.Text()
	}
	return fmt.Sprintf("V2-%02d-%s", shard, rand.Text())
}

func orderShard(id string) redisShard {
	if len(id) == 32 && strings.HasPrefix(id, "V2-") {
		n, _ := strconv.ParseUint(id[3:5], 10, 8)
		return redisShard(n)
	}
	return 0
}

func (s *Service) redisFor(shard redisShard) redis.Cmdable {
	if shard != 0 && s.shardRedis != nil {
		return s.shardRedis
	}
	return s.redis
}

func (s *Service) routes() []redisShard {
	if routes := s.queueShards.Load(); routes != nil {
		return *routes
	}
	return []redisShard{0}
}

func (s *Service) refreshRoutes(ctx context.Context) {
	operation, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var shards []redisShard
	if err := s.db.WithContext(operation).Table("seckill_activities").Distinct().
		Where("redis_shard BETWEEN 1 AND ?", shardCount).Pluck("redis_shard", &shards).Error; err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("refresh seckill queue routes", "error", err)
		}
		return
	}
	routes := append([]redisShard{0}, shards...)
	s.queueShards.Store(&routes)
}

func (s *Service) watchRoutes(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshRoutes(ctx)
		}
	}
}
