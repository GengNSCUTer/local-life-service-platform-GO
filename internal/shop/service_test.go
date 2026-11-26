package shop

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"slices"
	"strconv"
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

func TestGeoIndexReadinessAfterEviction(t *testing.T) {
	addr := os.Getenv("HMDP_SHOP_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set HMDP_SHOP_TEST_REDIS_ADDR to an isolated Redis server")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cache := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	defer cache.Close()
	key := "test:geo:{" + rand.Text() + "}"
	keys := []string{key, key + ":ready", key + ":build"}
	defer cache.Del(ctx, keys...)
	if err := cache.Set(ctx, keys[1], "1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if got, err := geoReady.Run(ctx, cache, keys[:2]).Int(); err != nil || got != 0 {
		t.Fatalf("evicted index readiness = %d, %v", got, err)
	}
	if err := searchGeoPage.Run(ctx, cache, keys[:2], 0, 0, geoRadiusMeters, 0, geoPageSize).Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("evicted search returned %v, want redis.Nil", err)
	}
	if got, err := claimGeoBuild.Run(ctx, cache, keys, "builder", 5000).Int(); err != nil || got != 1 {
		t.Fatalf("claim after eviction = %d, %v", got, err)
	}
	if got, err := finishGeoBuild.Run(ctx, cache, keys, "builder", 60000, 61000).Int(); err != nil || got != 1 {
		t.Fatalf("finish an empty index = %d, %v", got, err)
	}
	if got, err := geoReady.Run(ctx, cache, keys[:2]).Int(); err != nil || got != 1 {
		t.Fatalf("intentionally empty index readiness = %d, %v", got, err)
	}
	if got, err := searchGeoPage.Run(ctx, cache, keys[:2], 0, 0, geoRadiusMeters, 0, geoPageSize).StringSlice(); err != nil || len(got) != 0 {
		t.Fatalf("empty index search = %v, %v", got, err)
	}

	points := make([]*redis.GeoLocation, 0, 12)
	for i := 1; i <= 12; i++ {
		points = append(points, &redis.GeoLocation{Name: strconv.Itoa(i), Longitude: float64(i) / 1000})
	}
	if err := cache.GeoAdd(ctx, key, points...).Err(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set(ctx, keys[1], "1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		offset int
		want   []string
	}{{5, []string{"6", "7", "8", "9", "10"}}, {10, []string{"11", "12"}}, {15, nil}} {
		page, err := searchGeoPage.Run(ctx, cache, keys[:2], 0, 0, geoRadiusMeters, test.offset, geoPageSize).StringSlice()
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for i := 0; i < len(page); i += 2 {
			ids = append(ids, page[i])
		}
		if !slices.Equal(ids, test.want) {
			t.Fatalf("offset %d: ids = %v, want %v", test.offset, ids, test.want)
		}
	}
}
