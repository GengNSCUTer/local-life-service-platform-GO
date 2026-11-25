package social

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestNotificationRecovery(t *testing.T) {
	dsn, addr := os.Getenv("HMDP_SOCIAL_TEST_DSN"), os.Getenv("HMDP_SOCIAL_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set isolated MySQL and Redis addresses with migrations through 017")
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
	cache := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	defer cache.Close()
	ctx := t.Context()
	s := New(db, cache, slog.Default())
	exec := func(query string, args ...any) {
		t.Helper()
		if err := db.WithContext(ctx).Exec(query, args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		if err := db.WithContext(ctx).Raw(query, args...).Row().Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	const viewer, ordinary, removed, popular = 7001, 7002, 7003, 7004
	const firstPush, lastPush, pullPost, pastPost = 8001, 8100, 8301, 8302
	const fanoutPost, expiredPost = 8303, 8304
	exec("INSERT INTO social_follows (user_id, follow_user_id) VALUES (?, ?), (?, ?)", viewer, ordinary, viewer, removed)
	posts := make([]Blog, 0, 205)
	for i := range 100 {
		posts = append(posts, Blog{ID: uint64(firstPush + i), UserID: ordinary}, Blog{ID: uint64(8200 + i), UserID: removed})
	}
	posts = append(posts, Blog{ID: pullPost, UserID: popular, NotificationMode: notificationPull}, Blog{ID: pastPost, UserID: ordinary}, Blog{ID: fanoutPost, UserID: 7005}, Blog{ID: expiredPost, UserID: 7005})
	if err := db.WithContext(ctx).Table("tb_blog").CreateInBatches(posts, 100).Error; err != nil {
		t.Fatal(err)
	}
	exec("UPDATE tb_blog SET create_time = CURRENT_TIMESTAMP - INTERVAL 40 DAY WHERE id IN (?, ?)", pastPost, expiredPost)
	exec("DELETE FROM social_follows WHERE user_id = ? AND follow_user_id = ?", viewer, removed)
	exec("INSERT INTO social_follows (user_id, follow_user_id) VALUES (?, ?)", viewer, popular)
	key := notificationKey(viewer)
	stale := make([]redis.Z, 100)
	for i := range stale {
		stale[i] = redis.Z{Score: float64(time.Now().UnixMilli()), Member: fmt.Sprint(8200 + i)}
	}
	if err := cache.ZAdd(ctx, key, stale...).Err(); err != nil {
		t.Fatal(err)
	}
	token := rand.Text()
	if err := cache.HSet(ctx, "login:token:"+token, "id", viewer).Err(); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	users := user.New(db, cache, slog.Default(), false)
	s.Register(router, users.RequireLogin(), users.OptionalLogin())
	read := func() {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/blog/notifications", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		var payload struct{ Data []blogView }
		err := json.Unmarshal(res.Body.Bytes(), &payload)
		if res.Code != 200 || err != nil || len(payload.Data) != 100 || payload.Data[0].ID != pullPost || payload.Data[1].ID != lastPush || payload.Data[99].ID != firstPush+1 {
			t.Fatalf("unexpected notification page: status=%d len=%d error=%v", res.Code, len(payload.Data), err)
		}
	}
	read()
	if err := cache.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	read()
	if n, err := cache.ZCard(ctx, key).Result(); err != nil || n != 99 {
		t.Fatalf("rebuilt cache: %d, %v", n, err)
	}
	if err := cache.ZRem(ctx, key, fmt.Sprint(lastPush)).Err(); err != nil {
		t.Fatal(err)
	}
	read()
	if err := cache.ZScore(ctx, key, fmt.Sprint(lastPush)).Err(); err != nil {
		t.Fatalf("read repair did not restore a missing member: %v", err)
	}
	closedCache := redis.NewClient(&redis.Options{Addr: addr})
	closedCache.Close()
	s.ConfigureNotificationCache(closedCache)
	read()
	s.ConfigureNotificationCache(cache)

	// Seed 4,096 followers in ID order, then prove every shard can make independent progress.
	exec(`INSERT INTO social_follows (user_id, follow_user_id)
		WITH RECURSIVE n AS (SELECT 0 AS i UNION ALL SELECT i+1 FROM n WHERE i<63)
		SELECT 10000 + a.i*64 + b.i, 7005 FROM n a CROSS JOIN n b`)
	event := publishedBlog{BlogID: fanoutPost, AuthorID: 7005, CreatedAt: time.Now()}
	for range 2 {
		if err := s.enqueueFanout(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if n := count("SELECT COUNT(*) FROM social_fanout_shards WHERE blog_id = ?", fanoutPost); n != 8 {
		t.Fatalf("got %d fanout shards", n)
	}
	s.ConfigureNotificationCache(closedCache)
	exec("UPDATE social_fanout_shards SET due_at = CURRENT_TIMESTAMP(3) + INTERVAL 1 HOUR WHERE blog_id = ?", fanoutPost)
	for attempt := 1; attempt <= fanoutMaxAttempts; attempt++ {
		exec("UPDATE social_fanout_shards SET due_at = CURRENT_TIMESTAMP(3) WHERE blog_id = ? AND shard = 0", fanoutPost)
		outcome, err := s.fanoutStep(ctx)
		want := "retry"
		if attempt == fanoutMaxAttempts {
			want = "failed"
		}
		if err != nil || outcome != want {
			t.Fatalf("attempt %d: %q, %v; want %q", attempt, outcome, err, want)
		}
	}
	if n := count("SELECT COUNT(*) FROM social_fanout_shards WHERE blog_id = ? AND failed_at IS NOT NULL AND due_at IS NULL AND last_follower = 9999", fanoutPost); n != 1 {
		t.Fatal("failed shard did not retain its cursor")
	}
	s.ConfigureNotificationCache(cache)
	exec("UPDATE social_fanout_shards SET failed_at = NULL, attempts = 0, due_at = CURRENT_TIMESTAMP(3) WHERE blog_id = ?", fanoutPost)
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for {
				_, err := s.fanoutStep(ctx)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	workers.Wait()
	if n := count("SELECT COUNT(*) FROM social_fanout_shards WHERE blog_id = ? AND (due_at IS NOT NULL OR failed_at IS NOT NULL)", fanoutPost); n != 0 {
		t.Fatalf("%d unfinished shards", n)
	}
	pipe := cache.Pipeline()
	for id := 10000; id < 14096; id++ {
		pipe.ZScore(ctx, notificationKey(uint64(id)), fmt.Sprint(fanoutPost))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("missing notification in a fanout shard: %v", err)
	}
	if err := s.enqueueFanout(ctx, publishedBlog{BlogID: expiredPost, AuthorID: 7005, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if n := count("SELECT COUNT(*) FROM social_fanout_jobs WHERE blog_id = ?", expiredPost); n != 0 {
		t.Fatal("expired event recreated a job")
	}
	exec("UPDATE social_fanout_jobs SET created_at = CURRENT_TIMESTAMP - INTERVAL 38 DAY WHERE blog_id = ?", fanoutPost)
	maintenance, stop := context.WithCancel(ctx)
	var janitor sync.WaitGroup
	janitor.Go(func() { s.pruneFanout(maintenance) })
	defer func() { stop(); janitor.Wait() }()
	deadline := time.Now().Add(3 * time.Second)
	for count("SELECT COUNT(*) FROM social_fanout_jobs WHERE blog_id = ?", fanoutPost) != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := count("SELECT COUNT(*) FROM social_fanout_shards WHERE blog_id = ?", fanoutPost); n != 0 {
		t.Fatalf("cleanup left %d expired shards", n)
	}
}
