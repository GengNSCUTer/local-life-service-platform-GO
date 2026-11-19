package seckill

import (
	"context"
	"errors"
	"fmt"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	errOrderNotFound = errors.New("order not found")
	errOrderState    = errors.New("order state cannot be cancelled")
)

type cancellationResult struct {
	Order               Order `json:"order"`
	CompensationPending bool  `json:"compensationPending"`
}

func (s *Service) cancelOrder(ctx context.Context, event orderEvent, reason string) (Order, error) {
	var cancelled Order
	for attempt := 0; attempt < 3; attempt++ {
		err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var existing Order
			err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Table("seckill_orders").
				Take(&existing, "id = ?", event.ID).Error
			switch {
			case err == nil:
				if existing.UserID != event.UserID || existing.VoucherID != event.VoucherID {
					return errOrderNotFound
				}
				switch existing.Status {
				case statusCancelled:
					cancelled = existing
				case statusPending:
					now := time.Now().UTC().Truncate(time.Millisecond)
					if err := tx.Table("seckill_orders").Where("id = ? AND status = ?", event.ID, statusPending).
						Updates(map[string]any{"status": statusCancelled, "cancelled_at": now}).Error; err != nil {
						return err
					}
					if err := tx.Table("seckill_order_claims").Where("order_id = ?", event.ID).Delete(&orderClaim{}).Error; err != nil {
						return err
					}
					existing.Status = statusCancelled
					existing.CancelledAt = &now
					cancelled = existing
				case statusConfirmed:
					now := time.Now().UTC().Truncate(time.Millisecond)
					update := tx.Table("seckill_orders").Where("id = ? AND status = ?", event.ID, statusConfirmed).
						Updates(map[string]any{"status": statusCancelled, "cancelled_at": now})
					if update.Error != nil {
						return update.Error
					}
					stock := tx.Table("seckill_activities").Where("id = ? AND remaining_stock < total_stock", event.VoucherID).
						UpdateColumn("remaining_stock", gorm.Expr("remaining_stock + 1"))
					if stock.Error != nil {
						return stock.Error
					}
					if stock.RowsAffected == 0 {
						var activity Activity
						if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Table("seckill_activities").
							Take(&activity, "id = ?", event.VoucherID).Error; err != nil {
							return err
						}
						if activity.RemainingStock != activity.TotalStock {
							return errors.New("MySQL stock cannot be restored")
						}
					}
					if err := tx.Table("seckill_order_claims").Where("order_id = ?", event.ID).Delete(&orderClaim{}).Error; err != nil {
						return err
					}
					existing.Status = statusCancelled
					existing.CancelledAt = &now
					cancelled = existing
				default:
					return errOrderState
				}
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
				ON DUPLICATE KEY UPDATE reason = VALUES(reason)
			`, event.ID, reason).Error
		})
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

func (s *Service) syncResolution(ctx context.Context, event orderEvent) error {
	err := s.compensate(ctx, event)
	if err != nil {
		message := err.Error()
		if len(message) > 512 {
			message = message[:512]
		}
		if updateErr := s.db.WithContext(ctx).Table("seckill_order_resolutions").Where("order_id = ?", event.ID).
			Updates(map[string]any{"attempts": gorm.Expr("attempts + 1"), "last_error": message}).Error; updateErr != nil {
			return errors.Join(err, fmt.Errorf("record compensation failure: %w", updateErr))
		}
		return err
	}
	return s.markResolutionSynced(ctx, event.ID)
}

func (s *Service) markResolutionSynced(ctx context.Context, orderID string) error {
	return s.db.WithContext(ctx).Table("seckill_order_resolutions").Where("order_id = ?", orderID).
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
