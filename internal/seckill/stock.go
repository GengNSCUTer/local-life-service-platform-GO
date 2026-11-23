package seckill

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// BucketRemainingSQL takes the stock balance from the authoritative layout.
// The outer query must alias seckill_activities as a.
const BucketRemainingSQL = `CASE WHEN a.stock_buckets = 0 THEN a.remaining_stock ELSE
    (SELECT SUM(b.remaining_stock) FROM seckill_stock_buckets b WHERE b.voucher_id = a.id) END`

type stockBucket struct {
	VoucherID      uint64
	Bucket         uint8
	TotalStock     uint32
	RemainingStock uint32
}

func (s *Service) ConfigureActivity(activity Activity) Activity {
	if s.layout.Sharded {
		activity.RedisShard = activityShard(activity.ID)
	}
	if s.layout.StockBuckets > 1 && activity.TotalStock > 1 {
		activity.StockBuckets = uint8(min(int(activity.TotalStock), s.layout.StockBuckets, 32))
	}
	return activity
}

// InsertActivity creates the activity and its buckets inside the caller's transaction.
func InsertActivity(tx *gorm.DB, activity Activity) error {
	if err := tx.Table("seckill_activities").Create(&activity).Error; err != nil || activity.StockBuckets == 0 {
		return err
	}
	count := uint32(activity.StockBuckets)
	buckets := make([]stockBucket, activity.StockBuckets)
	for i := range buckets {
		stock := activity.TotalStock / count
		if uint32(i) < activity.TotalStock%count {
			stock++
		}
		buckets[i] = stockBucket{VoucherID: activity.ID, Bucket: uint8(i), TotalStock: stock, RemainingStock: stock}
	}
	return tx.Table("seckill_stock_buckets").Create(&buckets).Error
}

func findInventoryLayout(ctx context.Context, db *gorm.DB, voucherID uint64) (Activity, error) {
	var activity Activity
	err := db.WithContext(ctx).Table("seckill_activities").Select("id, redis_shard, stock_buckets").Take(&activity, "id = ?", voucherID).Error
	return activity, err
}

func insertOrderClaim(tx *gorm.DB, order Order) error {
	claim := orderClaim{VoucherID: order.VoucherID, UserID: order.UserID, OrderID: order.ID, CreatedAt: order.CreatedAt}
	return tx.Table("seckill_order_claims").Create(&claim).Error
}

func persistBucketOrder(tx *gorm.DB, order Order, count uint8) error {
	// Take the order and buyer locks before the inventory lock. Cancellation takes
	// the same order lock, and confirms across buckets share no exclusive activity lock.
	start := uint8(order.UserID % uint64(count))
	order.StockBucket = &start
	if err := tx.Table("seckill_orders").Create(&order).Error; err != nil {
		return err
	}
	if err := insertOrderClaim(tx, order); err != nil {
		return err
	}
	for offset := range count {
		bucket := (start + offset) % count
		update := tx.Table("seckill_stock_buckets").Where("voucher_id = ? AND bucket = ? AND remaining_stock > 0", order.VoucherID, bucket).
			UpdateColumn("remaining_stock", gorm.Expr("remaining_stock - 1"))
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected == 1 {
			if offset == 0 {
				return nil
			}
			return tx.Table("seckill_orders").Where("id = ?", order.ID).UpdateColumn("stock_bucket", bucket).Error
		}
	}
	return errInventoryMismatch
}

func restoreOrderStock(tx *gorm.DB, order Order) error {
	stock := tx.Table("seckill_activities").Where("id = ?", order.VoucherID)
	if order.StockBucket != nil {
		stock = tx.Table("seckill_stock_buckets").Where("voucher_id = ? AND bucket = ?", order.VoucherID, *order.StockBucket)
	}
	update := stock.Where("remaining_stock < total_stock").UpdateColumn("remaining_stock", gorm.Expr("remaining_stock + 1"))
	if update.Error != nil {
		return update.Error
	}
	if update.RowsAffected != 1 {
		return errors.New("confirmed order stock cannot be restored")
	}
	return nil
}
