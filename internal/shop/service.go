package shop

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	mathrand "math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	cacheTimeout  = 200 * time.Millisecond
	shopTTL       = 5 * time.Minute
	missingTTL    = 30 * time.Second
	invalidateTTL = time.Minute
	missingValue  = "null"
	refreshPrefix = "refresh:"
)

// Fill only if the key still contains what the reader saw before querying MySQL.
var fillCache = redis.NewScript(`
local current = redis.call('GET', KEYS[1]) or ''
if current ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
`)

type Service struct {
	shops   repository
	cache   *redis.Client
	logger  *slog.Logger
	adminID uint64
	loads   [64]chan struct{}
	breaker *shopBreaker
}

func New(db *gorm.DB, cache *redis.Client, logger *slog.Logger, adminID uint64) *Service {
	service := &Service{shops: repository{db: db}, cache: cache, logger: logger, adminID: adminID}
	for i := range service.loads {
		service.loads[i] = make(chan struct{}, 1)
	}
	return service
}

func cacheKey(id uint64) string { return "cache:shop:" + strconv.FormatUint(id, 10) }

func (s *Service) readCache(ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, cacheTimeout)
	defer cancel()
	value, err := s.cache.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return value, err
}

func cachedShop(value string, id uint64) (shopRecord, error, bool) {
	if value == missingValue {
		return shopRecord{}, gorm.ErrRecordNotFound, true
	}
	var item shopRecord
	if strings.HasPrefix(value, "{") && json.Unmarshal([]byte(value), &item) == nil && item.ID == id {
		return item, nil, true
	}
	return shopRecord{}, nil, false
}

func (s *Service) detail(ctx context.Context, id uint64) (shopRecord, error) {
	key := cacheKey(id)
	observed, cacheErr := s.readCache(ctx, key)
	if item, err, hit := cachedShop(observed, id); hit {
		return item, err
	}
	// Serialize cache fills for the same shop within this process. Waiting callers can cancel.
	gate := s.loads[id%uint64(len(s.loads))]
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return shopRecord{}, ctx.Err()
	}
	if cacheErr == nil {
		observed, cacheErr = s.readCache(ctx, key)
		if item, err, hit := cachedShop(observed, id); hit {
			return item, err
		}
	}
	item, err := s.breaker.find(ctx, s.shops, id)
	if cacheErr == nil && (err == nil || errors.Is(err, gorm.ErrRecordNotFound)) {
		s.saveDetail(ctx, key, observed, item, err)
	}
	return item, err
}

func (s *Service) saveDetail(ctx context.Context, key, observed string, item shopRecord, lookupErr error) {
	value := missingValue
	ttl := missingTTL
	if lookupErr == nil {
		payload, err := json.Marshal(item)
		if err != nil {
			s.logger.Warn("encode shop cache", "error", err)
			return
		}
		value = string(payload)
		ttl = shopTTL + time.Duration(mathrand.IntN(60))*time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, cacheTimeout)
	defer cancel()
	if err := fillCache.Run(ctx, s.cache, []string{key}, observed, value, ttl.Milliseconds()).Err(); err != nil {
		s.logger.Warn("fill shop cache", "key", key, "error", err)
	}
}

func (s *Service) invalidate(ctx context.Context, id uint64) {
	// The MySQL commit already succeeded. Try invalidation even if the caller disconnects.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cacheTimeout)
	defer cancel()
	if err := s.cache.Set(ctx, cacheKey(id), refreshPrefix+rand.Text(), invalidateTTL).Err(); err != nil {
		s.logger.Warn("invalidate shop cache after commit", "shop_id", id, "error", err)
	}
}
