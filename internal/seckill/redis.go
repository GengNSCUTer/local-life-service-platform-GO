package seckill

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "seckill:{orders}:"
const outboxKey = keyPrefix + "outbox"
const timeoutKey = keyPrefix + "timeouts"

var ErrStockWindowStarted = errors.New("Redis stock must be initialized before the activity starts")

var initActivity = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local existing = redis.call('HMGET', KEYS[1], 'stock', 'starts_at', 'ends_at')
if existing[1] then
    if existing[2] ~= ARGV[2] or existing[3] ~= ARGV[3] then
        return redis.error_reply('existing activity window differs')
    end
    return 1
end
if now >= tonumber(ARGV[2]) then
    return 0
end
redis.call('HSET', KEYS[1], 'stock', ARGV[1], 'starts_at', ARGV[2], 'ends_at', ARGV[3])
return 1
`)

var reserveStock = redis.NewScript(`
local previous = redis.call('HGET', KEYS[2], ARGV[1])
if previous then
    return {'duplicate', previous}
end
local activity = redis.call('HMGET', KEYS[1], 'stock', 'starts_at', 'ends_at')
if not activity[1] then
    return {'missing', ''}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
if now < tonumber(activity[2]) or now >= tonumber(activity[3]) then
    return {'closed', ''}
end
if tonumber(activity[1]) <= 0 then
    return {'sold_out', ''}
end
if redis.call('EXISTS', KEYS[3]) == 1 then
    return redis.error_reply('order ID collision')
end
redis.call('HINCRBY', KEYS[1], 'stock', -1)
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('SET', KEYS[3], ARGV[3])
redis.call('ZADD', KEYS[4], now, ARGV[2])
redis.call('ZADD', KEYS[5], now + tonumber(ARGV[4]), ARGV[2])
return {'accepted', ARGV[2]}
`)

var claimOutbox = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
if #due == 0 then
    return nil
end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[1]), due[1])
return due[1]
`)

var claimTimeout = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
if #due == 0 then
    return nil
end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[1]), due[1])
return due[1]
`)

var compensateReservation = redis.NewScript(`
local resolutionKey = KEYS[3]
if redis.call('EXISTS', resolutionKey) == 1 then
    return 0
end
if redis.call('EXISTS', KEYS[1]) == 0 then
    return redis.error_reply('activity is missing')
end
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
    return redis.error_reply('reservation does not match resolution event')
end
redis.call('HINCRBY', KEYS[1], 'stock', 1)
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[4])
redis.call('ZREM', KEYS[5], ARGV[2])
redis.call('ZREM', KEYS[6], ARGV[2])
redis.call('SET', resolutionKey, 'compensated')
return 1
`)

var finalizeReservation = redis.NewScript(`
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`)

var reconcileInventoryReservation = redis.NewScript(`
local resolutionKey = KEYS[3]
if redis.call('GET', resolutionKey) == 'inventory_reconciled' then
    return 0
end
if redis.call('EXISTS', KEYS[1]) == 0 then
    return redis.error_reply('activity is missing')
end
local buyer = redis.call('HGET', KEYS[2], ARGV[1])
if buyer and buyer ~= ARGV[2] then
    return redis.error_reply('reservation does not match inventory event')
end
redis.call('HSET', KEYS[1], 'stock', ARGV[3])
if buyer == ARGV[2] then
    redis.call('HDEL', KEYS[2], ARGV[1])
end
redis.call('DEL', KEYS[4])
redis.call('ZREM', KEYS[5], ARGV[2])
redis.call('ZREM', KEYS[6], ARGV[2])
redis.call('SET', resolutionKey, 'inventory_reconciled')
return 1
`)

func activityKey(id uint64) string {
	return keyPrefix + "activity:" + strconv.FormatUint(id, 10)
}

func orderKey(id string) string { return keyPrefix + "order:" + id }

func (s *Service) compensate(ctx context.Context, event orderEvent) error {
	key := activityKey(event.VoucherID)
	_, err := compensateReservation.Run(ctx, s.redis,
		[]string{key, key + ":buyers", keyPrefix + "resolution:" + event.ID, orderKey(event.ID), outboxKey, timeoutKey},
		strconv.FormatUint(event.UserID, 10), event.ID).Int()
	return err
}

func (s *Service) finalize(ctx context.Context, id string) error {
	return finalizeReservation.Run(ctx, s.redis,
		[]string{orderKey(id), outboxKey, timeoutKey}, id).Err()
}

func (s *Service) reconcileInventory(ctx context.Context, event orderEvent, remaining uint32) error {
	key := activityKey(event.VoucherID)
	_, err := reconcileInventoryReservation.Run(ctx, s.redis,
		[]string{key, key + ":buyers", keyPrefix + "resolution:" + event.ID, orderKey(event.ID), outboxKey, timeoutKey},
		strconv.FormatUint(event.UserID, 10), event.ID, remaining).Int()
	return err
}

func (s *Service) Prepare(ctx context.Context, activity Activity) error {
	activity.StartsAt = activity.StartsAt.UTC().Truncate(time.Millisecond)
	activity.EndsAt = activity.EndsAt.UTC().Truncate(time.Millisecond)
	if activity.ID == 0 || activity.TotalStock == 0 || !activity.EndsAt.After(activity.StartsAt) {
		return errors.New("positive voucher ID, positive stock, and a valid activity window are required")
	}
	activity.RemainingStock = activity.TotalStock
	if err := createActivity(ctx, s.db, activity); err != nil {
		return fmt.Errorf("prepare MySQL activity: %w", err)
	}
	return s.InitializeStock(ctx, activity)
}

// InitializeStock prewarms persisted activity stock before sales open.
// An existing Redis activity keeps its current stock.
func (s *Service) InitializeStock(ctx context.Context, activity Activity) error {
	initialized, err := initActivity.Run(ctx, s.redis, []string{activityKey(activity.ID)},
		activity.TotalStock, activity.StartsAt.UnixMilli(), activity.EndsAt.UnixMilli()).Int()
	if err != nil {
		return fmt.Errorf("prepare Redis stock: %w", err)
	}
	if initialized == 0 {
		return ErrStockWindowStarted
	}
	return nil
}
