package voucher

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	stockTasksTable = "voucher_stock_init_tasks"
	stockPending    = "pending"
	stockReady      = "ready"
	stockMissed     = "missed_window"
)

type stockInitTask struct {
	VoucherID uint64
	Attempts  uint64
}

// RunStockInitialization retries committed tasks until Redis confirms stock or the window is missed.
func (s *Service) RunStockInitialization(ctx context.Context) {
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.processStockInitialization(attempt)
		cancel()
		if err == nil {
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) && ctx.Err() == nil {
			s.logger.Warn("process voucher stock initialization", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (s *Service) processStockInitialization(ctx context.Context) error {
	var task stockInitTask
	token := rand.Text()
	db := s.repo.db.WithContext(ctx)
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Table(stockTasksTable).Select("voucher_id, attempts").Where("due_at <= CURRENT_TIMESTAMP(3)").
			Order("due_at, voucher_id").Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&task).Error; err != nil {
			return err
		}
		return tx.Exec(`UPDATE voucher_stock_init_tasks SET lease_token = ?, attempts = attempts + 1,
			due_at = TIMESTAMPADD(SECOND, 10, CURRENT_TIMESTAMP(3)) WHERE voucher_id = ?`, token, task.VoucherID).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}

	// Release the row lock before contacting Redis, and leave time to save the result.
	initCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	initErr := s.initializeStock(initCtx, task.VoucherID)
	cancel()

	updates := map[string]any{"status": stockReady, "due_at": nil, "lease_token": nil}
	switch {
	case errors.Is(initErr, seckill.ErrStockWindowStarted):
		updates["status"] = stockMissed
	case initErr != nil:
		updates["status"] = stockPending
		delaySeconds := 1 << min(task.Attempts, 3)
		updates["due_at"] = gorm.Expr("TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3))", delaySeconds)
	}
	result := db.Table(stockTasksTable).Where("voucher_id = ? AND lease_token = ?", task.VoucherID, token).Updates(updates)
	if result.Error == nil && result.RowsAffected == 1 && initErr != nil {
		s.logger.Warn("voucher stock initialization failed", "voucher_id", task.VoucherID,
			"attempt", task.Attempts+1, "status", updates["status"], "error", initErr)
	}
	return result.Error
}
