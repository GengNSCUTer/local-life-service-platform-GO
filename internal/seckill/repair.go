package seckill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var previewCompensation = redis.NewScript(`
local stock = tonumber(redis.call('HGET', KEYS[1], 'stock'))
local total = tonumber(ARGV[3])
if not stock or stock < 0 or stock > total then
    return 'invalid_stock'
end
if redis.call('HLEN', KEYS[2]) + stock ~= total then
    return 'inconsistent_buyers'
end
if redis.call('EXISTS', KEYS[3]) == 1 then
    return 'ready_to_sync'
end
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
    return 'inconsistent_buyers'
end
return 'ready'`)

type RepairResult struct {
	OrderID   string `json:"orderId"`
	VoucherID uint64 `json:"voucherId,omitempty"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
}

// RepairCompensation previews or retries one verifiable cancellation resolution.
func (s *Service) RepairCompensation(ctx context.Context, id string, apply bool) (result RepairResult, repairErr error) {
	result = RepairResult{OrderID: id, Status: "error"}
	if !orderIDPattern.MatchString(id) {
		return result, errors.New("invalid order ID")
	}
	if apply {
		pool, err := s.db.DB()
		if err != nil {
			return result, err
		}
		attempt, err := pool.ExecContext(ctx, "INSERT INTO seckill_repair_attempts (order_id, outcome) VALUES (?, 'started')", id)
		if err != nil {
			return result, fmt.Errorf("record repair attempt: %w", err)
		}
		attemptID, err := attempt.LastInsertId()
		if err != nil || attemptID <= 0 {
			return result, errors.Join(err, sql.ErrNoRows)
		}
		defer func() {
			auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			_, err := pool.ExecContext(auditCtx, "UPDATE seckill_repair_attempts SET outcome = ? WHERE id = ?", result.Status, attemptID)
			if err != nil {
				repairErr = errors.Join(repairErr, fmt.Errorf("record repair attempt: %w", err))
			}
		}()
	}
	order, err := loadOrder(ctx, s.db, id)
	if errors.Is(err, errOrderNotFound) {
		result.Status, result.Reason = "manual_review", "order_missing"
		return result, nil
	}
	if err != nil {
		return result, err
	}
	result.VoucherID = order.VoucherID
	if order.Status != statusCancelled {
		result.Status, result.Reason = "manual_review", "order_not_cancelled"
		return result, nil
	}
	var resolution struct {
		Reason      string
		RedisSynced bool
	}
	err = s.db.WithContext(ctx).Table("seckill_order_resolutions").Take(&resolution, "order_id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		result.Status, result.Reason = "manual_review", "resolution_missing"
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if resolution.RedisSynced {
		result.Status = "already_synced"
		return result, nil
	}
	switch resolution.Reason {
	case reasonUserCancelled, reasonPendingTimeout, reasonOrderConflict:
	default:
		result.Status, result.Reason = "manual_review", "ambiguous_resolution"
		return result, nil
	}
	var activity Activity
	if err := s.db.WithContext(ctx).Table("seckill_activities").Take(&activity, "id = ?", order.VoucherID).Error; err != nil {
		return result, err
	}
	var claims int64
	if err := s.db.WithContext(ctx).Table("seckill_order_claims").Where("order_id = ?", id).Count(&claims).Error; err != nil {
		return result, err
	}
	shard := orderShard(id)
	if claims != 0 || activity.RedisShard != shard {
		result.Status, result.Reason = "manual_review", "inconsistent_order_facts"
		return result, nil
	}
	key := shard.activity(order.VoucherID)
	stockTotal := strconv.FormatUint(uint64(activity.TotalStock), 10)
	outcome, err := previewCompensation.Run(ctx, s.redisFor(shard), []string{key, key + ":buyers", shard.prefix() + "resolution:" + id},
		strconv.FormatUint(order.UserID, 10), id, stockTotal).Text()
	if redis.HasErrorPrefix(err, "WRONGTYPE") {
		result.Status, result.Reason = "manual_review", "invalid_redis_keys"
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if outcome != "ready" && outcome != "ready_to_sync" {
		result.Status, result.Reason = "manual_review", outcome
		return result, nil
	}
	if !apply {
		result.Status = outcome
		return result, nil
	}
	if _, err := s.syncResolutionWithGuard(ctx, order.event(), stockTotal); err != nil {
		if errors.Is(err, errReservationMismatch) {
			result.Status, result.Reason = "manual_review", "redis_changed_after_preview"
			return result, nil
		}
		return result, err
	}
	result.Status = "synced"
	return result, nil
}
