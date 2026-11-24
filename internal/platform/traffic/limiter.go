package traffic

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var takeRequest = redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1])) or 0
if current >= tonumber(ARGV[1]) then
    return math.max(1, redis.call('TTL', KEYS[1]))
end
if redis.call('INCR', KEYS[1]) == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0`)

type Limiter struct {
	cache    *redis.Client
	outcomes *prometheus.CounterVec
}

func NewLimiter(cache *redis.Client, reg prometheus.Registerer) Limiter {
	outcomes := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hmdp_http_rate_limit_total", Help: "Rate limit decisions by policy and outcome."}, []string{"policy", "outcome"})
	reg.MustRegister(outcomes)
	return Limiter{cache: cache, outcomes: outcomes}
}

func (l Limiter) CodeByIP(limit int) gin.HandlerFunc {
	return l.middleware("auth_code_ip", limit, func(c *gin.Context) string {
		ip, _, err := net.SplitHostPort(c.Request.RemoteAddr)
		if err != nil {
			return c.Request.RemoteAddr
		}
		return ip
	})
}

func (l Limiter) SeckillByUser(limit int) gin.HandlerFunc {
	return l.middleware("seckill_user", limit, func(c *gin.Context) string {
		id, _ := user.CurrentID(c.Request.Context())
		return strconv.FormatUint(id, 10)
	})
}

func (l Limiter) middleware(policy string, limit int, identity func(*gin.Context) string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if limit <= 0 {
			c.Next()
			return
		}
		key := fmt.Sprintf("traffic:limit:%s:%x", policy, sha256.Sum256([]byte(identity(c))))
		ctx, cancel := context.WithTimeout(c.Request.Context(), 75*time.Millisecond)
		retry, err := takeRequest.Run(ctx, l.cache, []string{key}, limit, 60).Int()
		cancel()
		switch {
		case err != nil:
			l.outcomes.WithLabelValues(policy, "error").Inc()
			c.Abort()
			httpx.Respond(c, http.StatusServiceUnavailable, "request quota temporarily unavailable", nil)
		case retry > 0:
			l.outcomes.WithLabelValues(policy, "rejected").Inc()
			c.Abort()
			c.Header("Retry-After", strconv.Itoa(retry))
			httpx.Respond(c, http.StatusTooManyRequests, "request quota exceeded", nil)
		default:
			l.outcomes.WithLabelValues(policy, "allowed").Inc()
			c.Next()
		}
	}
}
