package seckill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	errOrderNotFound = errors.New("order not found")
	errOrderState    = errors.New("order state cannot be cancelled")
)

const (
	reasonUserCancelled     = "user_cancelled"
	reasonPendingTimeout    = "pending_timeout"
	reasonOrderConflict     = "order_conflict"
	reasonInventoryMismatch = "inventory_mismatch"
	reasonInvalidEvent      = "invalid_order_event"
)

type cancellationResult struct {
	Order               Order `json:"order"`
	CompensationPending bool  `json:"compensationPending"`
}

func (s *Service) cancelOrder(ctx context.Context, event orderEvent, reason string) (Order, error) {
	var cancelled Order
	for attempt := 0; attempt < 3; attempt++ {
		err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// Both paths lock the activity before inserting or updating an order.
			var activity Activity
			lock := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Table("seckill_activities").Select("id").
				Take(&activity, "id = ?", event.VoucherID).Error
			if errors.Is(lock, gorm.ErrRecordNotFound) {
				return errOrderNotFound
			}
			if lock != nil {
				return lock
			}
			var existing Order
			err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Table("seckill_orders").
				Take(&existing, "id = ?", event.ID).Error
			switch {
			case err == nil:
				if existing.UserID != event.UserID || existing.VoucherID != event.VoucherID {
					return errOrderNotFound
				}
				cancelled = existing
				if existing.Status == statusCancelled || (existing.Status == statusConfirmed && reason != reasonUserCancelled) {
					return nil
				}
				if existing.Status != statusPending && existing.Status != statusConfirmed {
					return errOrderState
				}
				if existing.Status == statusConfirmed {
					stock := tx.Table("seckill_activities").Where("id = ? AND remaining_stock < total_stock", event.VoucherID).
						UpdateColumn("remaining_stock", gorm.Expr("remaining_stock + 1"))
					if stock.Error != nil {
						return stock.Error
					}
					if stock.RowsAffected != 1 {
						return errors.New("confirmed order stock cannot be restored")
					}
				}
				now := time.Now().UTC().Truncate(time.Millisecond)
				if err := tx.Table("seckill_orders").Where("id = ?", event.ID).
					Updates(map[string]any{"status": statusCancelled, "cancelled_at": now}).Error; err != nil {
					return err
				}
				if err := tx.Table("seckill_order_claims").Where("order_id = ?", event.ID).Delete(&orderClaim{}).Error; err != nil {
					return err
				}
				cancelled.Status = statusCancelled
				cancelled.CancelledAt = &now
			case errors.Is(err, gorm.ErrRecordNotFound):
				now := time.Now().UTC().Truncate(time.Millisecond)
				cancelled = event.order(statusCancelled)
				cancelled.CancelledAt = &now
				if err := tx.Table("seckill_orders").Create(&cancelled).Error; err != nil {
					return err
				}
			default:
				return err
			}
			return tx.Exec(`
				INSERT INTO seckill_order_resolutions (order_id, reason, redis_synced)
				VALUES (?, ?, FALSE)
			`, event.ID, reason).Error
		}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err == nil {
			return cancelled, nil
		}
		if !retryableTransaction(err) {
			return Order{}, err
		}
	}
	return Order{}, errors.New("cancel order transaction retries exhausted")
}

func retryableTransaction(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == 1062 || mysqlErr.Number == 1205 || mysqlErr.Number == 1213)
}

func (s *Service) syncResolution(ctx context.Context, event orderEvent) (reason string, syncErr error) {
	started := time.Now()
	defer func() { s.observe(started, "compensation", operationOutcome(syncErr)) }()
	var state struct {
		Reason      string
		RedisSynced bool
	}
	if err := s.db.WithContext(ctx).Table("seckill_order_resolutions").
		Select("reason, redis_synced").Take(&state, "order_id = ?", event.ID).Error; err != nil {
		return state.Reason, err
	}
	if state.RedisSynced {
		return state.Reason, nil
	}
	stockLimit := ""
	if state.Reason == reasonInventoryMismatch {
		var activity Activity
		if err := s.db.WithContext(ctx).Table("seckill_activities").Select("remaining_stock").
			Take(&activity, "id = ?", event.VoucherID).Error; err != nil {
			return state.Reason, err
		}
		stockLimit = strconv.FormatUint(uint64(activity.RemainingStock), 10)
	}
	err := s.compensate(ctx, event, stockLimit)
	if err != nil {
		message := err.Error()
		if len(message) > 512 {
			message = message[:512]
		}
		if updateErr := s.db.WithContext(ctx).Table("seckill_order_resolutions").Where("order_id = ? AND redis_synced = FALSE", event.ID).
			Updates(map[string]any{"attempts": gorm.Expr("attempts + 1"), "last_error": message}).Error; updateErr != nil {
			return state.Reason, errors.Join(err, fmt.Errorf("record compensation failure: %w", updateErr))
		}
		return state.Reason, err
	}
	return state.Reason, s.markResolutionSynced(ctx, event.ID)
}

func (s *Service) markResolutionSynced(ctx context.Context, orderID string) error {
	return s.db.WithContext(ctx).Table("seckill_order_resolutions").Where("order_id = ? AND redis_synced = FALSE", orderID).
		Updates(map[string]any{
			"redis_synced": true,
			"attempts":     gorm.Expr("attempts + 1"),
			"last_error":   nil,
		}).Error
}

func (s *Service) pendingResolutions(ctx context.Context, limit int) ([]orderEvent, error) {
	events := make([]orderEvent, 0, limit)
	err := s.db.WithContext(ctx).Table("seckill_order_resolutions AS r").
		Select("1 AS version, o.id, o.voucher_id, o.user_id, o.created_at").
		Joins("JOIN seckill_orders AS o ON o.id = r.order_id").
		Where("r.redis_synced = FALSE AND o.status = ?", statusCancelled).
		Order("r.updated_at ASC").Limit(limit).Find(&events).Error
	return events, err
}
