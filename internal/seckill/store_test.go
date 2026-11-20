package seckill

import (
	"context"
	"crypto/rand"
	"os"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestConcurrentConfirmationAndTimeoutKeepStockConsistent(t *testing.T) {
	dsn := os.Getenv("HMDP_SECKILL_TEST_DSN")
	if dsn == "" {
		t.Skip("set HMDP_SECKILL_TEST_DSN to an isolated database with migrations 001 and 006")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	const stock = 24
	now := time.Now().UTC().Truncate(time.Millisecond)
	activity := Activity{ID: uint64(now.UnixNano()), TotalStock: stock, RemainingStock: stock,
		StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Minute)}
	if err := createActivity(ctx, db, activity); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, query := range []string{
			"DELETE r FROM seckill_order_resolutions r JOIN seckill_orders o ON o.id = r.order_id WHERE o.voucher_id = ?",
			"DELETE FROM seckill_order_claims WHERE voucher_id = ?",
			"DELETE FROM seckill_orders WHERE voucher_id = ?",
			"DELETE FROM seckill_activities WHERE id = ?",
		} {
			if err := db.WithContext(cleanup).Exec(query, activity.ID).Error; err != nil {
				t.Error(err)
			}
		}
	}()
	service := &Service{db: db}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range stock {
		event := orderEvent{Version: 1, ID: rand.Text(), VoucherID: activity.ID, UserID: uint64(i + 1), CreatedAt: now}
		workers.Go(func() {
			<-start
			if err := persistOrder(ctx, db, event); err != nil {
				t.Errorf("confirm %s: %v", event.ID, err)
			}
		})
		workers.Go(func() {
			<-start
			if _, err := service.cancelOrder(ctx, event, reasonPendingTimeout); err != nil {
				t.Errorf("expire %s: %v", event.ID, err)
			}
		})
	}
	close(start)
	workers.Wait()
	var orders, confirmed, claims int64
	for dest, query := range map[*int64]string{
		&orders:    "SELECT COUNT(*) FROM seckill_orders WHERE voucher_id = ?",
		&confirmed: "SELECT COUNT(*) FROM seckill_orders WHERE voucher_id = ? AND status = 'confirmed'",
		&claims:    "SELECT COUNT(*) FROM seckill_order_claims WHERE voucher_id = ?",
	} {
		if err := db.WithContext(ctx).Raw(query, activity.ID).Row().Scan(dest); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.WithContext(ctx).Table("seckill_activities").Take(&activity, "id = ?", activity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if orders != stock || claims != confirmed || confirmed+int64(activity.RemainingStock) != stock {
		t.Fatalf("orders=%d confirmed=%d claims=%d remaining=%d", orders, confirmed, claims, activity.RemainingStock)
	}
}

func TestPersistOrderCanBeReplayedAfterStockReachesZero(t *testing.T) {
	dsn := os.Getenv("HMDP_SECKILL_TEST_DSN")
	if dsn == "" {
		t.Skip("set HMDP_SECKILL_TEST_DSN to an isolated database with migrations 001 and 006")
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
		if err := db.WithContext(cleanup).Exec("DELETE r FROM seckill_order_resolutions r JOIN seckill_orders o ON o.id = r.order_id WHERE o.voucher_id = ?", activityID).Error; err != nil {
			t.Error(err)
		}
		if err := db.WithContext(cleanup).Exec("DELETE FROM seckill_order_claims WHERE voucher_id = ?", activityID).Error; err != nil {
			t.Error(err)
		}
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

func TestCancelledOrderCanBeReplayedAndBuyerCanPurchaseAgain(t *testing.T) {
	dsn := os.Getenv("HMDP_SECKILL_TEST_DSN")
	if dsn == "" {
		t.Skip("set HMDP_SECKILL_TEST_DSN to an isolated database with migrations 001 and 006")
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
		ID: activityID, TotalStock: 2, RemainingStock: 2,
		StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Minute),
	}
	if err := createActivity(ctx, db, activity); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		for _, query := range []string{
			"DELETE r FROM seckill_order_resolutions r JOIN seckill_orders o ON o.id = r.order_id WHERE o.voucher_id = ?",
			"DELETE FROM seckill_order_claims WHERE voucher_id = ?",
			"DELETE FROM seckill_orders WHERE voucher_id = ?",
			"DELETE FROM seckill_activities WHERE id = ?",
		} {
			if err := db.WithContext(cleanup).Exec(query, activityID).Error; err != nil {
				t.Error(err)
			}
		}
	}()

	service := &Service{db: db}
	first := orderEvent{Version: 1, ID: rand.Text(), VoucherID: activityID, UserID: 7, CreatedAt: now}
	if err := persistOrder(ctx, db, first); err != nil {
		t.Fatalf("persist first order: %v", err)
	}
	if _, err := service.cancelOrder(ctx, first, reasonUserCancelled); err != nil {
		t.Fatalf("cancel first order: %v", err)
	}
	if err := persistOrder(ctx, db, first); err != nil {
		t.Fatalf("replay cancelled event: %v", err)
	}

	second := orderEvent{Version: 1, ID: rand.Text(), VoucherID: activityID, UserID: 7, CreatedAt: now.Add(time.Millisecond)}
	if err := persistOrder(ctx, db, second); err != nil {
		t.Fatalf("persist second order for same buyer: %v", err)
	}

	var stored Activity
	if err := db.WithContext(ctx).Table("seckill_activities").Take(&stored, "id = ?", activityID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.RemainingStock != 1 {
		t.Fatalf("remaining stock = %d, want 1", stored.RemainingStock)
	}
	var claim orderClaim
	if err := db.WithContext(ctx).Table("seckill_order_claims").Take(&claim,
		"voucher_id = ? AND user_id = ?", activityID, 7).Error; err != nil {
		t.Fatal(err)
	}
	if claim.OrderID != second.ID {
		t.Fatalf("active claim = %s, want %s", claim.OrderID, second.ID)
	}
}
