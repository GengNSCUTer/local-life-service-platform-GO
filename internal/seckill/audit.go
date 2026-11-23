package seckill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	auditConsistent = "consistent"
	auditRepairable = "repairable"
	auditManual     = "manual_review"
	auditBlocked    = "blocked"
)

var redisInventorySnapshot = redis.NewScript(`
local activity = redis.call('HMGET', KEYS[1], 'stock', 'starts_at', 'ends_at')
return {activity[1], activity[2], activity[3], redis.call('HLEN', KEYS[2])}
`)

type InventoryAudit struct {
	VoucherID          uint64    `json:"voucherId"`
	CheckedAt          time.Time `json:"checkedAt"`
	Status             string    `json:"status"`
	Detail             string    `json:"detail"`
	TotalStock         int64     `json:"totalStock"`
	MySQLRemaining     int64     `json:"mysqlRemaining"`
	ConfirmedOrders    int64     `json:"confirmedOrders"`
	MySQLClaims        int64     `json:"mysqlClaims"`
	PendingResolutions int64     `json:"pendingResolutions"`
	RedisRemaining     *int64    `json:"redisRemaining"`
	RedisBuyers        *int64    `json:"redisBuyers"`
}

type redisInventory struct {
	stock, buyers, startsAt, endsAt int64
	present, valid                  bool
}

func (s *Service) readRedisInventory(ctx context.Context, voucherID uint64, shard redisShard) (redisInventory, error) {
	key := shard.activity(voucherID)
	fields, err := redisInventorySnapshot.Run(ctx, s.redisFor(shard), []string{key, key + ":buyers"}).Slice()
	if redis.HasErrorPrefix(err, "WRONGTYPE") {
		return redisInventory{present: true}, nil
	}
	if err != nil {
		return redisInventory{}, err
	}
	if len(fields) != 4 {
		return redisInventory{}, errors.New("unexpected Redis inventory snapshot")
	}
	buyers, ok := fields[3].(int64)
	if !ok {
		return redisInventory{}, errors.New("unexpected Redis buyer count")
	}
	if fields[0] == nil && fields[1] == nil && fields[2] == nil {
		return redisInventory{buyers: buyers}, nil
	}
	values := [3]int64{}
	for i := range values {
		field, ok := fields[i].(string)
		if !ok {
			return redisInventory{buyers: buyers, present: true}, nil
		}
		values[i], err = strconv.ParseInt(field, 10, 64)
		if err != nil {
			return redisInventory{buyers: buyers, present: true}, nil
		}
	}
	return redisInventory{stock: values[0], buyers: buyers, startsAt: values[1], endsAt: values[2], present: true, valid: true}, nil
}

// AuditInventory records the latest observation. The existing resolution worker owns stock repairs.
func (s *Service) AuditInventory(ctx context.Context, voucherID uint64) (InventoryAudit, error) {
	result := InventoryAudit{VoucherID: voucherID, CheckedAt: time.Now().UTC().Truncate(time.Microsecond)}
	activity, err := findInventoryLayout(ctx, s.db, voucherID)
	if err != nil {
		return result, err
	}
	readCache := func() (redisInventory, error) {
		redisCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		return s.readRedisInventory(redisCtx, voucherID, activity.RedisShard)
	}
	before, beforeErr := readCache()
	var row struct {
		TotalStock, MySQLRemaining, ConfirmedOrders, MySQLClaims, PendingResolutions int64
		StartsAt, EndsAt                                                             time.Time
		BucketCount, BucketTotal, InvalidBuckets, InvalidAssignments                 int64
	}
	err = s.db.WithContext(ctx).Raw(`
		SELECT a.total_stock, COALESCE(`+BucketRemainingSQL+`, 0), a.starts_at, a.ends_at,
		       (SELECT COUNT(*) FROM seckill_orders o WHERE o.voucher_id = a.id AND o.status = 'confirmed') AS confirmed_orders,
		       (SELECT COUNT(*) FROM seckill_order_claims c WHERE c.voucher_id = a.id),
		       (SELECT COUNT(*) FROM seckill_orders o JOIN seckill_order_resolutions r ON r.order_id = o.id
		        WHERE o.voucher_id = a.id AND o.status = 'cancelled' AND NOT r.redis_synced) AS pending_resolutions,
		       (SELECT COUNT(*) FROM seckill_stock_buckets b WHERE b.voucher_id = a.id),
		       (SELECT COALESCE(SUM(b.total_stock), 0) FROM seckill_stock_buckets b WHERE b.voucher_id = a.id),
		       (SELECT COUNT(*) FROM seckill_stock_buckets b
		        LEFT JOIN (SELECT stock_bucket, COUNT(*) AS orders FROM seckill_orders
		                   WHERE voucher_id = ? AND status = 'confirmed' GROUP BY stock_bucket) assigned ON assigned.stock_bucket = b.bucket
		        WHERE b.voucher_id = a.id AND (b.bucket >= a.stock_buckets OR b.remaining_stock > b.total_stock OR
		          CAST(b.total_stock AS SIGNED) - CAST(b.remaining_stock AS SIGNED) <> COALESCE(assigned.orders, 0))),
		       (SELECT COUNT(*) FROM seckill_orders o WHERE o.voucher_id = a.id AND o.status = 'confirmed'
		        AND (o.stock_bucket IS NULL OR o.stock_bucket >= a.stock_buckets))
		FROM seckill_activities a WHERE a.id = ?
	`, voucherID, voucherID).Row().Scan(&row.TotalStock, &row.MySQLRemaining, &row.StartsAt, &row.EndsAt,
		&row.ConfirmedOrders, &row.MySQLClaims, &row.PendingResolutions, &row.BucketCount, &row.BucketTotal, &row.InvalidBuckets, &row.InvalidAssignments)
	if err != nil {
		return result, fmt.Errorf("read MySQL activity %d: %w", voucherID, err)
	}
	result.TotalStock, result.MySQLRemaining, result.ConfirmedOrders = row.TotalStock, row.MySQLRemaining, row.ConfirmedOrders
	result.MySQLClaims, result.PendingResolutions = row.MySQLClaims, row.PendingResolutions
	if activity.StockBuckets > 0 && (row.BucketCount != int64(activity.StockBuckets) || row.BucketTotal != row.TotalStock || row.InvalidBuckets != 0 || row.InvalidAssignments != 0) {
		result.Status, result.Detail = auditManual, "mysql_stock_bucket_mismatch"
	} else if beforeErr != nil {
		result.Status, result.Detail = auditBlocked, "redis_unavailable"
	} else {
		after, afterErr := readCache()
		switch {
		case afterErr != nil:
			result.Status, result.Detail = auditBlocked, "redis_unavailable"
		case before != after:
			result.Status, result.Detail = auditBlocked, "redis_changed_during_check"
		default:
			result.RedisBuyers = &after.buyers
			if after.valid {
				result.RedisRemaining = &after.stock
			}
			result.Status, result.Detail = classifyInventory(result, after, row.StartsAt.UnixMilli(), row.EndsAt.UnixMilli())
		}
	}
	if err := s.saveInventoryAudit(ctx, result); err != nil {
		return result, err
	}
	if s.metrics != nil {
		s.metrics.audits.WithLabelValues(result.Status).Inc()
	}
	return result, nil
}

func classifyInventory(audit InventoryAudit, snapshot redisInventory, startsAt, endsAt int64) (string, string) {
	switch {
	case audit.MySQLRemaining > audit.TotalStock || audit.ConfirmedOrders != audit.TotalStock-audit.MySQLRemaining:
		return auditManual, "mysql_stock_mismatch"
	case audit.MySQLClaims != audit.ConfirmedOrders:
		return auditManual, "mysql_claim_count_mismatch"
	case !snapshot.present:
		return auditManual, "redis_activity_missing"
	case !snapshot.valid || snapshot.startsAt != startsAt || snapshot.endsAt != endsAt:
		return auditManual, "redis_activity_invalid"
	case snapshot.stock < 0 || snapshot.stock > audit.TotalStock || snapshot.buyers != audit.TotalStock-snapshot.stock:
		return auditManual, "redis_stock_buyer_mismatch"
	case snapshot.buyers < audit.ConfirmedOrders:
		return auditManual, "redis_confirmed_buyers_missing"
	case audit.PendingResolutions > 0:
		return auditRepairable, "cancellation_resolutions_pending"
	default:
		return auditConsistent, "stock_counts_match"
	}
}

func (s *Service) saveInventoryAudit(ctx context.Context, audit InventoryAudit) error {
	// Compare timestamps before replacing fields so a slower replica cannot overwrite a newer sample.
	return s.db.WithContext(ctx).Exec(`
		INSERT INTO seckill_inventory_audits
		    (voucher_id, checked_at, status, detail, total_stock, mysql_remaining,
		     confirmed_orders, mysql_claims, pending_resolutions, redis_remaining, redis_buyers)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS sample
		ON DUPLICATE KEY UPDATE
		    status = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.status, seckill_inventory_audits.status),
		    detail = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.detail, seckill_inventory_audits.detail),
		    total_stock = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.total_stock, seckill_inventory_audits.total_stock),
		    mysql_remaining = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.mysql_remaining, seckill_inventory_audits.mysql_remaining),
		    confirmed_orders = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.confirmed_orders, seckill_inventory_audits.confirmed_orders),
		    mysql_claims = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.mysql_claims, seckill_inventory_audits.mysql_claims),
		    pending_resolutions = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.pending_resolutions, seckill_inventory_audits.pending_resolutions),
		    redis_remaining = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.redis_remaining, seckill_inventory_audits.redis_remaining),
		    redis_buyers = IF(sample.checked_at >= seckill_inventory_audits.checked_at, sample.redis_buyers, seckill_inventory_audits.redis_buyers),
		    checked_at = GREATEST(sample.checked_at, seckill_inventory_audits.checked_at)
	`, audit.VoucherID, audit.CheckedAt, audit.Status, audit.Detail, audit.TotalStock, audit.MySQLRemaining,
		audit.ConfirmedOrders, audit.MySQLClaims, audit.PendingResolutions, audit.RedisRemaining, audit.RedisBuyers).Error
}

func (s *Service) auditBatch(ctx context.Context, cursor uint64) (uint64, error) {
	const batchSize = 20
	var ids []uint64
	err := s.db.WithContext(ctx).Table("seckill_activities").Where("id > ?", cursor).Order("id").Limit(batchSize).Pluck("id", &ids).Error
	if err != nil {
		return cursor, err
	}
	var firstErr error
	for _, id := range ids {
		if ctx.Err() != nil {
			return cursor, ctx.Err()
		}
		remainingBudget := time.Second
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < remainingBudget {
			return cursor, firstErr
		}
		operation, cancel := context.WithTimeout(ctx, remainingBudget)
		audit, err := s.AuditInventory(operation, id)
		cancel()
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			if firstErr == nil {
				firstErr = err
			}
			s.logger.Warn("inventory check failed", "voucher_id", id, "error", err)
		}
		if audit.Status == auditManual {
			s.logger.Warn("inventory check requires manual review", "voucher_id", id, "detail", audit.Detail)
		}
		cursor = id
	}
	if len(ids) < batchSize {
		cursor = 0
	}
	return cursor, firstErr
}
