package shop

import (
	"context"
	"crypto/rand"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const visitorCookie = "hmdp_visitor"

var (
	uvZone         = time.FixedZone("UTC+08", 8*60*60)
	visitorPattern = regexp.MustCompile(`^[A-Z2-7]{26}$`)
	addVisitor     = redis.NewScript(`
redis.call('PFADD', KEYS[1], ARGV[1])
redis.call('EXPIREAT', KEYS[1], ARGV[2])
return 1`)
)

func (s *Service) recordVisit(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 25*time.Millisecond)
	defer cancel()
	now := time.Now().In(uvZone)
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, uvZone)
	visitor, _ := c.Cookie(visitorCookie)
	identity := ""
	if id, ok := user.CurrentID(ctx); ok {
		identity = "u:" + strconv.FormatUint(id, 10)
	} else {
		if !visitorPattern.MatchString(visitor) {
			visitor = rand.Text()
			http.SetCookie(c.Writer, &http.Cookie{Name: visitorCookie, Value: visitor, Path: "/shop", MaxAge: 30 * 86400, HttpOnly: true, Secure: c.Request.TLS != nil, SameSite: http.SameSiteLaxMode})
		}
		identity = "a:" + visitor
	}
	_ = addVisitor.Run(ctx, s.cache, []string{uvKey(day)}, identity, day.AddDate(0, 0, 30).Unix()).Err()
}

func uvKey(day time.Time) string { return "analytics:shop:uv:" + day.Format(time.DateOnly) }

func (s *Service) dailyUV(c *gin.Context) {
	day, err := time.ParseInLocation(time.DateOnly, c.Query("date"), uvZone)
	if err != nil {
		httpx.Respond(c, http.StatusBadRequest, "date must use YYYY-MM-DD", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), cacheTimeout)
	defer cancel()
	count, err := s.cache.PFCount(ctx, uvKey(day)).Result()
	if err != nil {
		httpx.Respond(c, http.StatusServiceUnavailable, "UV temporarily unavailable", nil)
		return
	}
	httpx.Respond(c, http.StatusOK, "", gin.H{"date": day.Format(time.DateOnly), "uv": count, "approximate": true})
}
