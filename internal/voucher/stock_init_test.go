package voucher

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestStockInitializationRecoversAfterRedisFailure(t *testing.T) {
	dsn := os.Getenv("HMDP_VOUCHER_TEST_DSN")
	addr := os.Getenv("HMDP_VOUCHER_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set MySQL and Redis test addresses for an isolated environment with migrations through 016")
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
	cache := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	defer cache.Close()
	unavailable := redis.NewClient(&redis.Options{
		Addr: addr, MaxRetries: -1, DialerRetries: 1,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("simulated Redis connection failure")
		},
	})
	defer unavailable.Close()
	newService := func(client *redis.Client) *Service {
		stock := seckill.New(db, client, slog.Default(), time.Minute, 0, 0, seckill.Layout{Sharded: true, StockBuckets: 4})
		return New(db, slog.Default(), 1, stock)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	shop := struct {
		ID      uint64 `gorm:"primaryKey"`
		Name    string
		TypeID  uint64
		Images  string
		Address string
		X, Y    float64
	}{Name: "stock init integration shop", TypeID: 1}
	if err := db.WithContext(ctx).Table("tb_shop").Create(&shop).Error; err != nil {
		t.Fatal(err)
	}
	begin := time.Now().Add(time.Minute)
	activity, err := activityFor(4, begin, begin.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	api := newService(unavailable)
	key := rand.Text()
	item := Voucher{ShopID: shop.ID, Title: "stock init integration voucher", Type: seckillVoucher, Status: activeStatus}
	id, err := api.create(ctx, item, &activity, key)
	if err != nil {
		t.Fatal(err)
	}
	stockKey := fmt.Sprintf("seckill:{orders:v2:%02d}:activity:%d", id%16+1, id)
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := cache.Del(cleanup, stockKey).Err(); err != nil {
			t.Error(err)
		}
		for _, query := range []string{
			"DELETE FROM voucher_create_requests WHERE voucher_id = ?",
			"DELETE FROM voucher_seckill_links WHERE voucher_id = ?",
			"DELETE FROM seckill_stock_buckets WHERE voucher_id = ?",
			"DELETE FROM seckill_activities WHERE id = ?",
			"DELETE FROM tb_voucher WHERE id = ?",
		} {
			if err := db.WithContext(cleanup).Exec(query, id).Error; err != nil {
				t.Error(err)
			}
		}
		if err := db.WithContext(cleanup).Exec("DELETE FROM tb_shop WHERE id = ?", shop.ID).Error; err != nil {
			t.Error(err)
		}
	}()
	if err := api.initialize(ctx, id); err == nil {
		t.Fatal("initialization succeeded during simulated outage")
	}
	if duplicateID, err := api.create(ctx, item, &activity, key); err != nil || duplicateID != id {
		t.Fatalf("idempotent create = %d, %v; want %d", duplicateID, err, id)
	}
	if err := api.processStockInitialization(ctx); err != nil {
		t.Fatal(err)
	}
	var task struct {
		Status   string
		Attempts uint64
	}
	readTask := func() {
		t.Helper()
		if err := db.WithContext(ctx).Table(stockTasksTable).Take(&task, "voucher_id = ?", id).Error; err != nil {
			t.Fatal(err)
		}
	}
	readTask()
	if task.Status != stockPending || task.Attempts != 1 {
		t.Fatalf("task during Redis outage = %+v", task)
	}

	worker := newService(cache)
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.RunStockInitialization(workerCtx)
	}()
	defer func() { stop(); <-done }()
	for task.Status != stockReady && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
		readTask()
	}
	if task.Status != stockReady || task.Attempts != 2 {
		t.Fatalf("task after Redis recovery = %+v", task)
	}
	if stock, err := cache.HGet(ctx, stockKey, "stock").Int(); err != nil || stock != 4 {
		t.Fatalf("Redis stock = %d, %v; want 4", stock, err)
	}
}
