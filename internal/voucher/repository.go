package voucher

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
	"gorm.io/gorm"
)

const (
	voucherTable  = "tb_voucher"
	requestsTable = "voucher_create_requests"
)

var (
	errUnknownShop = errors.New("shop not found")
	errKeyConflict = errors.New("idempotency key already has a different request")
	errIDInUse     = errors.New("voucher ID reserved by a development activity")
)

type Voucher struct {
	ID          uint64     `json:"id" gorm:"primaryKey"`
	ShopID      uint64     `json:"shopId"`
	Title       string     `json:"title"`
	SubTitle    *string    `json:"subTitle"`
	Rules       *string    `json:"rules"`
	PayValue    uint64     `json:"payValue"`
	ActualValue int64      `json:"actualValue"`
	Type        uint8      `json:"type"`
	Status      uint8      `json:"status"`
	CreatedAt   *time.Time `json:"createTime" gorm:"column:create_time;->"`
	UpdatedAt   *time.Time `json:"updateTime" gorm:"column:update_time;->"`
}

type listing struct {
	Voucher
	Stock     *uint32    `json:"stock,omitempty"`
	BeginTime *time.Time `json:"beginTime,omitempty"`
	EndTime   *time.Time `json:"endTime,omitempty"`
}

type savedRequest struct {
	RequestHash []byte
	VoucherID   *uint64
}

type repository struct{ db *gorm.DB }

func (r repository) listByShop(ctx context.Context, shopID uint64) ([]listing, error) {
	items := make([]listing, 0)
	err := r.db.WithContext(ctx).Table("tb_voucher AS v").
		Select("v.*, "+seckill.BucketRemainingSQL+" AS stock, a.starts_at AS begin_time, a.ends_at AS end_time").
		Joins("LEFT JOIN voucher_seckill_links AS link ON link.voucher_id = v.id").
		Joins("LEFT JOIN seckill_activities AS a ON a.id = link.voucher_id").
		Where("v.shop_id = ? AND v.status = ?", shopID, activeStatus).Order("v.id ASC").Find(&items).Error
	return items, err
}

func (r repository) create(ctx context.Context, item Voucher, activity *seckill.Activity, key string, fingerprint [32]byte, configure func(seckill.Activity) seckill.Activity) (uint64, error) {
	var id uint64
	for {
		err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if key != "" {
				existing, err := lockRequest(tx, key, fingerprint)
				if err != nil {
					return err
				}
				if existing != nil {
					id = *existing
					return nil
				}
			}
			if activity != nil && !activity.StartsAt.After(time.Now()) {
				return errInvalidWindow
			}
			var shop struct{ ID uint64 }
			if err := tx.Table("tb_shop").Select("id").Take(&shop, "id = ?", item.ShopID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return errUnknownShop
				}
				return err
			}
			item.ID = 0
			if err := tx.Table(voucherTable).Create(&item).Error; err != nil {
				return err
			}
			if err := attachActivity(tx, item.ID, activity, configure); err != nil {
				return err
			}
			if key != "" {
				if err := tx.Table(requestsTable).Where("request_key = ?", key).Update("voucher_id", item.ID).Error; err != nil {
					return err
				}
			}
			id = item.ID
			return nil
		})
		if !errors.Is(err, errIDInUse) {
			return id, err
		}
		// A rollback consumes the generated ID. The next insert can use another ID.
	}
}

func lockRequest(tx *gorm.DB, key string, fingerprint [32]byte) (*uint64, error) {
	if err := tx.Exec(`INSERT INTO voucher_create_requests (request_key, request_hash)
		VALUES (?, ?) ON DUPLICATE KEY UPDATE request_key = request_key`, key, fingerprint[:]).Error; err != nil {
		return nil, err
	}
	var previous savedRequest
	if err := tx.Table(requestsTable).Take(&previous, "request_key = ?", key).Error; err != nil {
		return nil, err
	}
	if !bytes.Equal(previous.RequestHash, fingerprint[:]) {
		return nil, errKeyConflict
	}
	return previous.VoucherID, nil
}

func attachActivity(tx *gorm.DB, id uint64, activity *seckill.Activity, configure func(seckill.Activity) seckill.Activity) error {
	var count int64
	if err := tx.Table("seckill_activities").Where("id = ?", id).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errIDInUse
	}
	if activity == nil {
		return nil
	}
	item := *activity
	item.ID = id
	if err := seckill.InsertActivity(tx, configure(item)); err != nil {
		return err
	}
	return tx.Exec("INSERT INTO voucher_seckill_links (voucher_id) VALUES (?)", id).Error
}

func (r repository) findActivity(ctx context.Context, id uint64) (seckill.Activity, error) {
	var result seckill.Activity
	err := r.db.WithContext(ctx).Table("seckill_activities AS a").Select("a.*").
		Joins("JOIN voucher_seckill_links AS link ON link.voucher_id = a.id").Take(&result, "a.id = ?", id).Error
	return result, err
}
