package social

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const pageSize = 10

var errInvalidShop = errors.New("shop not found")
var errInvalidFollow = errors.New("follow target must be another existing user")

type Service struct {
	db     *gorm.DB
	cache  *redis.Client
	logger *slog.Logger
}

func New(db *gorm.DB, cache *redis.Client, logger *slog.Logger) *Service {
	return &Service{db: db, cache: cache, logger: logger}
}

type blogInput struct {
	ShopID  uint64 `json:"shopId" binding:"required"`
	Title   string `json:"title" binding:"required,max=255"`
	Images  string `json:"images" binding:"max=2048"`
	Content string `json:"content" binding:"required,max=2048"`
}

type Blog struct {
	ID        uint64    `json:"id" gorm:"primaryKey"`
	ShopID    uint64    `json:"shopId"`
	UserID    uint64    `json:"userId"`
	Title     string    `json:"title"`
	Images    string    `json:"images"`
	Content   string    `json:"content"`
	Liked     uint32    `json:"liked"`
	Comments  uint32    `json:"comments"`
	CreatedAt time.Time `json:"createTime" gorm:"column:create_time;->"`
}

type blogView struct {
	Blog
	Name   string `json:"name"`
	Icon   string `json:"icon"`
	IsLike bool   `json:"isLike"`
}

type profile struct {
	ID       uint64 `json:"id"`
	NickName string `json:"nickName"`
	Icon     string `json:"icon"`
}

func (s *Service) publish(ctx context.Context, author uint64, input blogInput) (uint64, error) {
	item := Blog{ShopID: input.ShopID, UserID: author, Title: input.Title, Images: input.Images, Content: input.Content}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var shop struct{ ID uint64 }
		if err := tx.Table("tb_shop").Select("id").Take(&shop, "id = ?", input.ShopID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errInvalidShop
			}
			return err
		}
		if err := tx.Table("tb_blog").Create(&item).Error; err != nil {
			return err
		}
		return tx.Exec("INSERT INTO social_blog_outbox (blog_id, author_id) VALUES (?, ?)", item.ID, author).Error
	})
	return item.ID, err
}

func (s *Service) blogs(ctx context.Context, viewer uint64) *gorm.DB {
	return s.db.WithContext(ctx).Table("tb_blog AS b").
		Select("b.id, b.shop_id, b.user_id, b.title, b.images, b.content, COALESCE(b.liked, 0) AS liked, COALESCE(b.comments, 0) AS comments, b.create_time, COALESCE(u.nick_name, '') AS name, COALESCE(u.icon, '') AS icon, (l.user_id IS NOT NULL) AS is_like").
		Joins("LEFT JOIN tb_user u ON u.id = b.user_id").
		Joins("LEFT JOIN social_blog_likes l ON l.blog_id = b.id AND l.user_id = ?", viewer)
}

func (s *Service) changeLike(ctx context.Context, blogID, viewer uint64, desired *bool) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var item struct{ ID uint64 }
		if err := tx.Table("tb_blog").Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").Take(&item, "id = ?", blogID).Error; err != nil {
			return err
		}
		var count int64
		if err := tx.Table("social_blog_likes").Where("blog_id = ? AND user_id = ?", blogID, viewer).Count(&count).Error; err != nil {
			return err
		}
		wanted := count == 0
		if desired != nil {
			wanted = *desired
		}
		if wanted == (count != 0) {
			return nil
		}
		delta := 1
		if wanted {
			if err := tx.Exec("INSERT INTO social_blog_likes (blog_id, user_id) VALUES (?, ?)", blogID, viewer).Error; err != nil {
				return err
			}
		} else {
			delta = -1
			if err := tx.Exec("DELETE FROM social_blog_likes WHERE blog_id = ? AND user_id = ?", blogID, viewer).Error; err != nil {
				return err
			}
		}
		return tx.Exec("UPDATE tb_blog SET liked = GREATEST(CAST(COALESCE(liked, 0) AS SIGNED) + ?, 0) WHERE id = ?", delta, blogID).Error
	})
}
