package seckill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

const (
	statusPending   = "pending"
	statusConfirmed = "confirmed"
	statusReview    = "review_required"
)

var errOrderConflict = errors.New("order ID or buyer conflicts with an existing order")

type Activity struct {
	ID             uint64
	TotalStock     uint32
	RemainingStock uint32
	StartsAt       time.Time
	EndsAt         time.Time
}

type Order struct {
	ID        string    `json:"orderId"`
	VoucherID uint64    `json:"voucherId"`
	UserID    uint64    `json:"-"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

type orderEvent struct {
	Version   int       `json:"schema_version"`
	ID        string    `json:"order_id"`
	VoucherID uint64    `json:"voucher_id"`
	UserID    uint64    `json:"user_id"`
	CreatedAt time.Time `json:"occurred_at"`
}

func (e orderEvent) order(status string) Order {
	return Order{ID: e.ID, VoucherID: e.VoucherID, UserID: e.UserID, Status: status, CreatedAt: e.CreatedAt}
}

func createActivity(ctx context.Context, db *gorm.DB, activity Activity) error {
	if err := db.WithContext(ctx).Table("seckill_activities").Create(&activity).Error; err != nil {
		if !duplicateKey(err) {
			return err
		}
		var existing Activity
		if err := db.WithContext(ctx).Table("seckill_activities").First(&existing, "id = ?", activity.ID).Error; err != nil {
			return err
		}
		if existing.TotalStock != activity.TotalStock || !existing.StartsAt.Equal(activity.StartsAt) || !existing.EndsAt.Equal(activity.EndsAt) {
			return errors.New("activity ID already has different stock or times")
		}
	}
	return nil
}

func persistOrder(ctx context.Context, db *gorm.DB, event orderEvent) error {
	order := event.order(statusConfirmed)
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		update := tx.Table("seckill_activities").Where("id = ? AND remaining_stock > 0", order.VoucherID).
			UpdateColumn("remaining_stock", gorm.Expr("remaining_stock - 1"))
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected == 0 {
			order.Status = statusReview
		}
		// A duplicate insert rolls back the stock change above.
		return tx.Table("seckill_orders").Create(&order).Error
	})
	if !duplicateKey(err) {
		return err
	}

	var existing Order
	lookup := db.WithContext(ctx).Table("seckill_orders").Take(&existing, "voucher_id = ? AND user_id = ?", order.VoucherID, order.UserID).Error
	if errors.Is(lookup, gorm.ErrRecordNotFound) || (lookup == nil && existing.ID != order.ID) {
		return errOrderConflict
	}
	if lookup != nil {
		return fmt.Errorf("check order after duplicate key: %w", lookup)
	}
	return nil
}

func duplicateKey(err error) bool {
	var mysqlError *mysql.MySQLError
	return errors.As(err, &mysqlError) && mysqlError.Number == 1062
}
