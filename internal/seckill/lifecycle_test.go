package seckill

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestReservationRecovery(t *testing.T) {
	dsn := os.Getenv("HMDP_SECKILL_TEST_DSN")
	addr := os.Getenv("HMDP_SECKILL_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set MySQL and Redis test addresses for an isolated environment with migrations 001, 002, 006 and 007")
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
	cache := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	defer cache.Close()
	service := New(db, cache, slog.Default(), time.Minute, 0, 0)

	for _, route := range []string{"confirmed before timeout", "recover backup", "audit missing payload", "inventory mismatch never increases Redis stock"} {
		t.Run(route, func(t *testing.T) {
			ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
			defer stop()
			now := time.Now().UTC().Truncate(time.Millisecond)
			event := orderEvent{Version: 1, ID: rand.Text(), VoucherID: uint64(time.Now().UnixNano()), UserID: 1, CreatedAt: now}
			activity := activityKey(event.VoucherID)
			for _, err := range []error{
				db.WithContext(ctx).Table("seckill_activities").Create(&Activity{ID: event.VoucherID, TotalStock: 2, RemainingStock: 2, StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Minute)}).Error,
				cache.HSet(ctx, activity, "stock", 2, "starts_at", now.Add(-time.Minute).UnixMilli(), "ends_at", now.Add(time.Minute).UnixMilli()).Err(),
			} {
				if err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				if err := service.finalize(cleanup, event.ID); err != nil {
					t.Error(err)
				}
				if err := cache.Del(cleanup, activity, activity+":buyers", keyPrefix+"resolution:"+event.ID).Err(); err != nil {
					t.Error(err)
				}
				for _, query := range []string{
					"DELETE FROM seckill_reservation_anomalies WHERE order_id = ?",
					"DELETE FROM seckill_order_resolutions WHERE order_id = ?",
					"DELETE FROM seckill_order_claims WHERE order_id = ?",
					"DELETE FROM seckill_orders WHERE id = ?",
				} {
					if err := db.WithContext(cleanup).Exec(query, event.ID).Error; err != nil {
						t.Error(err)
					}
				}
				if err := db.WithContext(cleanup).Exec("DELETE FROM seckill_activities WHERE id = ?", event.VoucherID).Error; err != nil {
					t.Error(err)
				}
			}()

			payload, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			result, err := reserveStock.Run(ctx, cache,
				[]string{activity, activity + ":buyers", orderKey(event.ID), outboxKey, timeoutKey, reservationBackupKey(event.ID)},
				"1", event.ID, payload, time.Minute.Milliseconds()).StringSlice()
			if err != nil || len(result) != 2 || result[0] != "accepted" {
				t.Fatalf("reserve = %v, %v", result, err)
			}
			if err := cache.ZAdd(ctx, timeoutKey, redis.Z{Member: event.ID, Score: 0}).Err(); err != nil {
				t.Fatal(err)
			}

			wantStatus, wantStock := statusCancelled, 2
			switch route {
			case "confirmed before timeout":
				if err := persistOrder(ctx, db, event); err != nil {
					t.Fatal(err)
				}
				wantStatus, wantStock = statusConfirmed, 1
			case "recover backup":
				if err := cache.Set(ctx, orderKey(event.ID), "invalid-json", 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "audit missing payload":
				if err := cache.Del(ctx, orderKey(event.ID), reservationBackupKey(event.ID)).Err(); err != nil {
					t.Fatal(err)
				}
				wantStatus, wantStock = "", 1
			case "inventory mismatch never increases Redis stock":
				// Simulate another Redis pre-reservation while MySQL still reports two units.
				if err := cache.HSet(ctx, activity, "stock", 0).Err(); err != nil {
					t.Fatal(err)
				}
				outcome, err := service.resolveDeadLetterOrder(ctx, event, reasonInventoryMismatch)
				if err != nil || outcome != "inventory_reconciled" {
					t.Fatalf("DLQ outcome = %q, %v", outcome, err)
				}
				wantStock = 0
			}
			if route != "inventory mismatch never increases Redis stock" {
				if err := service.expireNext(ctx); err != nil {
					t.Fatal(err)
				}
			}
			order, err := loadOrder(ctx, db, event.ID)
			if wantStatus == "" {
				var count int64
				if !errors.Is(err, errOrderNotFound) {
					t.Fatalf("missing reservation created an order: %+v, %v", order, err)
				}
				if err := db.WithContext(ctx).Table("seckill_reservation_anomalies").Where("order_id = ?", event.ID).Count(&count).Error; err != nil || count != 1 {
					t.Fatalf("anomaly audit count = %d, %v", count, err)
				}
			} else if err != nil || order.Status != wantStatus {
				t.Fatalf("order status = %q, %v, want %s", order.Status, err, wantStatus)
			}
			if wantStatus == statusCancelled {
				if err := cache.Del(ctx, keyPrefix+"resolution:"+event.ID).Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := service.syncResolution(ctx, event); err != nil {
					t.Fatalf("repeat a synced resolution without Redis marker: %v", err)
				}
			}
			if stock, err := cache.HGet(ctx, activity, "stock").Int(); err != nil || stock != wantStock {
				t.Fatalf("Redis stock = %d, %v, want %d", stock, err, wantStock)
			}
			if _, err := cache.ZScore(ctx, timeoutKey, event.ID).Result(); !errors.Is(err, redis.Nil) {
				t.Fatalf("timeout entry still present: %v", err)
			}
		})
	}

	t.Run("late initial preparation can be retried with a future window", func(t *testing.T) {
		ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
		defer stop()
		now := time.Now().UTC()
		item := Activity{ID: uint64(now.UnixNano()), TotalStock: 2, StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour)}
		defer db.Exec("DELETE FROM seckill_activities WHERE id = ?", item.ID)
		defer cache.Del(t.Context(), activityKey(item.ID))
		if err := service.Prepare(ctx, item); !errors.Is(err, ErrStockWindowStarted) {
			t.Fatalf("prepare a new activity after the start returned %v", err)
		}
		var count int64
		if err := db.Table("seckill_activities").Where("id = ?", item.ID).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("rejected preparation left %d activities, query error %v", count, err)
		}
		item.StartsAt = now.Add(time.Minute)
		if err := service.Prepare(ctx, item); err != nil {
			t.Fatal(err)
		}
		if err := service.Prepare(ctx, item); err != nil {
			t.Fatalf("retry after successful preparation: %v", err)
		}
	})

	t.Run("malformed dead letter keeps consumer moving", func(t *testing.T) {
		message := kafka.Message{Partition: 0, Offset: time.Now().UnixNano(), Value: []byte(`{"order_id":"too long for the order ID database field"}`)}
		defer db.Exec("DELETE FROM seckill_dead_letters WHERE dlq_partition = ? AND dlq_offset = ?", message.Partition, message.Offset)
		if err := service.reconcileDeadLetter(t.Context(), message); err != nil {
			t.Fatal(err)
		}
	})
}
