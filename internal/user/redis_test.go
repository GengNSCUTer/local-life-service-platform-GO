package user

import (
	"context"
	"crypto/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestVerificationCodeCanOnlyBeConsumedOnce(t *testing.T) {
	addr := os.Getenv("HMDP_AUTH_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set HMDP_AUTH_TEST_REDIS_ADDR to an isolated Redis server")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	first := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	second := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	defer first.Close()
	defer second.Close()
	key := "test:login:code:{" + rand.Text() + "}"
	issueKeys := []string{key, key + ":cooldown"}
	defer func() {
		// Both test keys also expire if the Redis server is unavailable during cleanup.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := first.Del(cleanupCtx, issueKeys...).Err(); err != nil {
			t.Errorf("delete test keys: %v", err)
		}
	}()

	const code = "012345"
	issued, err := issueCode.Run(ctx, first, issueKeys, code, codeTTL.Milliseconds(), codeCooldown.Milliseconds()).Int()
	if err != nil || issued != 1 {
		t.Fatalf("issue code returned %d, %v", issued, err)
	}

	const parallelAttempts = 20
	var workers sync.WaitGroup
	matches := make(chan int, parallelAttempts)
	for _, cache := range []*redis.Client{first, second} {
		for range parallelAttempts / 2 {
			workers.Go(func() {
				matched, err := consumeCode.Run(ctx, cache, []string{key}, code, codeAttempts).Int()
				if err != nil {
					t.Errorf("consume code: %v", err)
					return
				}
				matches <- matched
			})
		}
	}
	workers.Wait()
	close(matches)

	successes := 0
	for matched := range matches {
		successes += matched
	}
	if successes != 1 {
		t.Fatalf("got %d successful verifications, want 1", successes)
	}
	issued, err = issueCode.Run(ctx, second, issueKeys, code, codeTTL.Milliseconds(), codeCooldown.Milliseconds()).Int()
	if err != nil || issued != 0 {
		t.Fatalf("issue code during cooldown returned %d, %v", issued, err)
	}
}
