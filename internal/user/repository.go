package user

import (
	"context"
	"crypto/rand"
	"errors"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

const userTable = "tb_user"

type account struct {
	ID       uint64 `gorm:"primaryKey"`
	Phone    string
	NickName string
}

type profile struct {
	ID       uint64 `json:"id"`
	NickName string `json:"nickName"`
	Icon     string `json:"icon"`
}

type repository struct {
	db *gorm.DB
}

func (r repository) lookup(ctx context.Context, condition string, value any) (profile, error) {
	var result profile
	// Java's original schema permits NULL display fields.
	err := r.db.WithContext(ctx).Table(userTable).
		Select("id, COALESCE(nick_name, '') AS nick_name, COALESCE(icon, '') AS icon").
		Take(&result, condition, value).Error
	return result, err
}

func (r repository) findOrCreate(ctx context.Context, phone string) (profile, error) {
	result, err := r.lookup(ctx, "phone = ?", phone)
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return result, err
	}
	newAccount := account{Phone: phone, NickName: "user_" + rand.Text()}
	err = r.db.WithContext(ctx).Table(userTable).Create(&newAccount).Error
	if err == nil {
		return profile{ID: newAccount.ID, NickName: newAccount.NickName}, nil
	}
	var duplicate *mysqldriver.MySQLError
	if errors.As(err, &duplicate) && duplicate.Number == 1062 {
		return r.lookup(ctx, "phone = ?", phone)
	}
	return profile{}, err
}
