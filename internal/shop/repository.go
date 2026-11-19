package shop

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	shopTable = "tb_shop"
	typeTable = "tb_shop_type"
)

var errUnknownType = errors.New("shop type not found")

type shopRecord struct {
	ID        uint64     `json:"id" gorm:"primaryKey"`
	Name      string     `json:"name"`
	TypeID    uint64     `json:"typeId"`
	Images    string     `json:"images"`
	Area      *string    `json:"area"`
	Address   string     `json:"address"`
	X         float64    `json:"x"`
	Y         float64    `json:"y"`
	AvgPrice  *uint64    `json:"avgPrice"`
	Sold      uint32     `json:"sold"`
	Comments  uint32     `json:"comments"`
	Score     uint32     `json:"score"`
	OpenHours *string    `json:"openHours"`
	CreatedAt *time.Time `json:"createTime" gorm:"column:create_time;->"`
	UpdatedAt *time.Time `json:"updateTime" gorm:"column:update_time;->"`
}

type shopType struct {
	ID   uint64  `json:"id" gorm:"primaryKey"`
	Name *string `json:"name"`
	Icon *string `json:"icon"`
	Sort *uint32 `json:"sort"`
}

type repository struct{ db *gorm.DB }

func (r repository) find(ctx context.Context, id uint64) (shopRecord, error) {
	var item shopRecord
	err := r.db.WithContext(ctx).Table(shopTable).Take(&item, "id = ?", id).Error
	return item, err
}

func (r repository) types(ctx context.Context) ([]shopType, error) {
	items := make([]shopType, 0)
	err := r.db.WithContext(ctx).Table(typeTable).Order("sort ASC, id ASC").Find(&items).Error
	return items, err
}

func (r repository) list(ctx context.Context, typeID uint64, name string, page, size int) ([]shopRecord, error) {
	items := make([]shopRecord, 0)
	query := r.db.WithContext(ctx).Table(shopTable)
	if typeID > 0 {
		query = query.Where("type_id = ?", typeID)
	}
	if name != "" {
		// Escape LIKE metacharacters so the search matches a literal substring.
		pattern := strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(name)
		query = query.Where("name LIKE ? ESCAPE '!'", "%"+pattern+"%")
	}
	err := query.Order("id ASC").Offset((page - 1) * size).Limit(size).Find(&items).Error
	return items, err
}

func requireType(db *gorm.DB, id uint64) error {
	var item shopType
	err := db.Table(typeTable).Select("id").Take(&item, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errUnknownType
	}
	return err
}

func (r repository) create(ctx context.Context, item *shopRecord) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := requireType(tx, item.TypeID); err != nil {
			return err
		}
		return tx.Table(shopTable).Create(item).Error
	})
}

func (r repository) update(ctx context.Context, id uint64, changes shopPatch) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing shopRecord
		if err := tx.Table(shopTable).Select("id").Take(&existing, "id = ?", id).Error; err != nil {
			return err
		}
		if changes.TypeID != nil {
			if err := requireType(tx, *changes.TypeID); err != nil {
				return err
			}
		}
		return tx.Table(shopTable).Where("id = ?", id).Updates(changes).Error
	})
}
