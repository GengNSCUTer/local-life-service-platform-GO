package seckill

import (
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestPersistOrderCanBeReplayedAfterStockReachesZero(t *testing.T) {
	dsn := os.Getenv("HMDP_SECKILL_TEST_DSN")
	if dsn == "" {
		t.Skip("set HMDP_SECKILL_TEST_DSN to an isolated database with the seckill migration")
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
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	activityID := uint64(now.UnixNano())
	activity := Activity{
		ID: activityID, TotalStock: 1, RemainingStock: 1,
		StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Minute),
	}
	if err := createActivity(ctx, db, activity); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := db.WithContext(cleanup).Exec("DELETE FROM seckill_orders WHERE voucher_id = ?", activityID).Error; err != nil {
			t.Error(err)
		}
		if err := db.WithContext(cleanup).Exec("DELETE FROM seckill_activities WHERE id = ?", activityID).Error; err != nil {
			t.Error(err)
		}
	}()
	event := orderEvent{Version: 1, ID: rand.Text(), VoucherID: activityID, UserID: 1, CreatedAt: now}
	if err := persistOrder(ctx, db, event); err != nil {
		t.Fatalf("persist first delivery: %v", err)
	}
	if err := persistOrder(ctx, db, event); err != nil {
		t.Fatalf("persist replay after stock reaches zero: %v", err)
	}
	var count int64
	if err := db.WithContext(ctx).Table("seckill_orders").Where("voucher_id = ?", activityID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("order count = %d, error = %v", count, err)
	}
	if err := db.WithContext(ctx).Table("seckill_activities").Take(&activity, "id = ?", activityID).Error; err != nil || activity.RemainingStock != 0 {
		t.Fatalf("remaining stock = %d, error = %v", activity.RemainingStock, err)
	}
}
