package voucher

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
	"gorm.io/gorm"
)

const (
	normalVoucher  uint8 = 0
	seckillVoucher uint8 = 1
	activeStatus   uint8 = 1
)

var errInvalidWindow = errors.New("activity start must be in the future and before its end")

type Service struct {
	repo    repository
	logger  *slog.Logger
	adminID uint64
	seckill *seckill.Service
}

func New(db *gorm.DB, logger *slog.Logger, adminID uint64, activities *seckill.Service) *Service {
	return &Service{repo: repository{db: db}, logger: logger, adminID: adminID, seckill: activities}
}

func (s *Service) create(ctx context.Context, item Voucher, activity *seckill.Activity, key string) (uint64, error) {
	var fingerprint [32]byte
	if key != "" {
		payload, err := json.Marshal(struct {
			Voucher  Voucher
			Activity *seckill.Activity
		}{item, activity})
		if err != nil {
			return 0, err
		}
		fingerprint = sha256.Sum256(payload)
	}
	return s.repo.create(ctx, item, activity, key, fingerprint, s.seckill.ConfigureActivity)
}

func (s *Service) initialize(ctx context.Context, id uint64) error {
	if err := s.initializeStock(ctx, id); err != nil {
		return err
	}
	if err := s.repo.db.WithContext(ctx).Exec(`UPDATE voucher_stock_init_tasks
		SET status = ?, due_at = NULL, lease_token = NULL WHERE voucher_id = ? AND status <> ?`,
		stockReady, id, stockReady).Error; err != nil {
		s.logger.Warn("acknowledge voucher stock initialization", "voucher_id", id, "error", err)
	}
	return nil
}

func (s *Service) initializeStock(ctx context.Context, id uint64) error {
	activity, err := s.repo.findActivity(ctx, id)
	if err != nil {
		return err
	}
	return s.seckill.InitializeStock(ctx, activity)
}

func activityFor(stock uint32, begin, end time.Time) (seckill.Activity, error) {
	begin = begin.UTC().Truncate(time.Millisecond)
	end = end.UTC().Truncate(time.Millisecond)
	if stock == 0 || !end.After(begin) || begin.Year() < 1000 || end.Year() > 9999 {
		return seckill.Activity{}, errors.New("positive stock and a valid activity window are required")
	}
	return seckill.Activity{TotalStock: stock, RemainingStock: stock, StartsAt: begin, EndsAt: end}, nil
}
