package social

import (
	"crypto/rand"
	"os"
	"testing"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestPublishChoosesModeFromCurrentFollowers(t *testing.T) {
	dsn := os.Getenv("HMDP_SOCIAL_TEST_DSN")
	if dsn == "" {
		t.Skip("set HMDP_SOCIAL_TEST_DSN to an isolated database with migrations applied")
	}
	db, err := gorm.Open(mysql.Open(dsn))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx := t.Context()
	tx := db.WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()

	var accounts [3]profile
	for i := range accounts {
		if err := tx.Exec("INSERT INTO tb_user (phone) VALUES (?)", rand.Text()[:11]).Error; err != nil {
			t.Fatal(err)
		}
		if err := tx.Raw("SELECT LAST_INSERT_ID() AS id").Scan(&accounts[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Exec("INSERT INTO tb_shop (name, type_id, images, address, x, y) VALUES ('mode test', 1, '', '', 0, 0)").Error; err != nil {
		t.Fatal(err)
	}
	var shopID uint64
	if err := tx.Raw("SELECT LAST_INSERT_ID()").Row().Scan(&shopID); err != nil {
		t.Fatal(err)
	}
	service := New(tx, nil, nil)
	service.ConfigureNotificationPull(2)
	author := accounts[0].ID
	follow := func(viewer uint64, desired bool) {
		t.Helper()
		if err := service.changeFollow(ctx, viewer, author, desired); err != nil {
			t.Fatal(err)
		}
	}
	assertMode := func(id uint64, wantMode uint8, wantOutbox int) {
		t.Helper()
		var mode uint8
		var outbox int
		err := tx.Raw(`SELECT notification_mode, EXISTS (SELECT 1 FROM social_blog_outbox WHERE blog_id = tb_blog.id)
			FROM tb_blog WHERE id = ?`, id).Row().Scan(&mode, &outbox)
		if err != nil || mode != wantMode || outbox != wantOutbox {
			t.Fatalf("blog %d: mode=%d outbox=%d error=%v, want mode=%d outbox=%d", id, mode, outbox, err, wantMode, wantOutbox)
		}
	}
	publish := func() uint64 {
		t.Helper()
		id, err := service.publish(ctx, author, blogInput{ShopID: shopID, Title: "mode test", Content: "mode test"})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	follow(accounts[1].ID, true)
	follow(accounts[1].ID, true)
	before := publish()
	assertMode(before, 0, 1)
	follow(accounts[2].ID, true)
	atThreshold := publish()
	assertMode(atThreshold, notificationPull, 0)
	follow(accounts[2].ID, false)
	assertMode(publish(), 0, 1)
	assertMode(before, 0, 1)
	assertMode(atThreshold, notificationPull, 0)

	follow(accounts[2].ID, true)
	service.ConfigureNotificationPull(0)
	assertMode(publish(), 0, 1)

	service.ConfigureNotificationPull(1)
	follow(accounts[1].ID, false)
	follow(accounts[2].ID, false)
	assertMode(publish(), 0, 1)
	follow(accounts[1].ID, true)
	assertMode(publish(), notificationPull, 0)
}
