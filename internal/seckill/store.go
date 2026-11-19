package seckill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	statusPending   = "pending"
	statusConfirmed = "confirmed"
	statusCancelled = "cancelled"
)

var errOrderConflict = errors.New("order ID or buyer conflicts with an existing order")
var errInventoryMismatch = errors.New("MySQL inventory does not match the seckill reservation")

type Activity struct {
	ID             uint64
	TotalStock     uint32
	RemainingStock uint32
	StartsAt       time.Time
	EndsAt         time.Time
}

type Order struct {
	ID          string     `json:"orderId"`
	VoucherID   uint64     `json:"voucherId"`
	UserID      uint64     `json:"-"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"createdAt"`
	CancelledAt *time.Time `json:"cancelledAt,omitempty"`
}

type orderClaim struct {
	VoucherID uint64
	UserID    uint64
	OrderID   string
	CreatedAt time.Time
}

type orderEvent struct {
	Version   int       `json:"schema_version"`
	ID        string    `json:"order_id"`
	VoucherID uint64    `json:"voucher_id"`
	UserID    uint64    `json:"user_id"`
	CreatedAt time.Time `json:"occurred_at"`
}

func (e orderEvent) valid(id string) bool {
	return e.Version == 1 && e.ID == id && orderIDPattern.MatchString(e.ID) && e.VoucherID > 0 && e.UserID > 0 &&
		e.CreatedAt.UTC().Year() >= 1000 && e.CreatedAt.UTC().Year() <= 9999
}

func (e orderEvent) order(status string) Order {
	return Order{ID: e.ID, VoucherID: e.VoucherID, UserID: e.UserID, Status: status, CreatedAt: e.CreatedAt}
}

func (o Order) event() orderEvent {
	return orderEvent{Version: 1, ID: o.ID, VoucherID: o.VoucherID, UserID: o.UserID, CreatedAt: o.CreatedAt}
}

func loadOrder(ctx context.Context, db *gorm.DB, id string) (Order, error) {
	var order Order
	err := db.WithContext(ctx).Table("seckill_orders").Take(&order, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Order{}, errOrderNotFound
	}
	return order, err
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
		var existing Order
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Table("seckill_orders").
			Take(&existing, "id = ?", order.ID).Error
		if err == nil {
			if existing.VoucherID == order.VoucherID && existing.UserID == order.UserID &&
				(existing.Status == statusConfirmed || existing.Status == statusCancelled) {
				return nil
			}
			return errOrderConflict
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		update := tx.Table("seckill_activities").Where("id = ? AND remaining_stock > 0", order.VoucherID).
			UpdateColumn("remaining_stock", gorm.Expr("remaining_stock - 1"))
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected == 0 {
			return errInventoryMismatch
		}
		if err := tx.Table("seckill_orders").Create(&order).Error; err != nil {
			return err
		}
		claim := orderClaim{VoucherID: order.VoucherID, UserID: order.UserID, OrderID: order.ID, CreatedAt: order.CreatedAt}
		// A duplicate claim rolls back the order and stock change above.
		return tx.Table("seckill_order_claims").Create(&claim).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if !duplicateKey(err) && !errors.Is(err, errInventoryMismatch) {
		return err
	}

	stored, lookupErr := loadOrder(ctx, db, order.ID)
	if lookupErr == nil {
		if stored.VoucherID == order.VoucherID && stored.UserID == order.UserID &&
			(stored.Status == statusConfirmed || stored.Status == statusCancelled) {
			return nil
		}
		return errOrderConflict
	}
	if !errors.Is(lookupErr, errOrderNotFound) {
		return lookupErr
	}
	var claim orderClaim
	lookup := db.WithContext(ctx).Table("seckill_order_claims").Take(&claim,
		"voucher_id = ? AND user_id = ?", order.VoucherID, order.UserID).Error
	if errors.Is(lookup, gorm.ErrRecordNotFound) {
		if errors.Is(err, errInventoryMismatch) {
			return err
		}
		return errOrderConflict
	}
	if lookup != nil {
		return fmt.Errorf("check order after duplicate key: %w", lookup)
	}
	if claim.OrderID != order.ID {
		return errOrderConflict
	}
	return nil
}

func duplicateKey(err error) bool {
	var mysqlError *mysql.MySQLError
	return errors.As(err, &mysqlError) && mysqlError.Number == 1062
}
