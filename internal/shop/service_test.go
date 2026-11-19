package shop

import (
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestInvalidationRejectsAnOlderCacheFill(t *testing.T) {
	addr := os.Getenv("HMDP_SHOP_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set HMDP_SHOP_TEST_REDIS_ADDR to an isolated Redis server")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cache := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	defer cache.Close()
	key := "test:cache:shop:" + rand.Text()
	marker := refreshPrefix + rand.Text()
	if err := cache.Set(ctx, key, marker, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	defer cache.Del(ctx, key)

	// A reader who saw a miss before the update must not refill with its old result.
	result, err := fillCache.Run(ctx, cache, []string{key}, "", `{"id":1,"name":"old"}`, shopTTL.Milliseconds()).Int()
	if err != nil || result != 0 {
		t.Fatalf("old fill returned %d, %v", result, err)
	}
	const updated = `{"id":1,"name":"updated"}`
	result, err = fillCache.Run(ctx, cache, []string{key}, marker, updated, shopTTL.Milliseconds()).Int()
	if err != nil || result != 1 {
		t.Fatalf("current fill returned %d, %v", result, err)
	}
	got, err := cache.Get(ctx, key).Result()
	if err != nil || got != updated {
		t.Fatalf("cache returned %q, %v", got, err)
	}
}
