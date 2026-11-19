package user

import (
	"context"
	"fmt"
	"math/bits"
	"net/http"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
)

var signZone = time.FixedZone("Asia/Shanghai", 8*60*60)

func signKey(ctx context.Context, day time.Time) string {
	id, _ := CurrentID(ctx)
	return fmt.Sprintf("sign:%d:%s", id, day.Format("200601"))
}

func (s *Service) sign(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	day := time.Now().In(signZone)
	if err := s.cache.SetBit(ctx, signKey(ctx, day), int64(day.Day()-1), 1).Err(); err != nil {
		s.unavailable(c, "sign in", err)
		return
	}
	httpx.Respond(c, http.StatusOK, "", nil)
}

func (s *Service) signCount(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	day := time.Now().In(signZone)
	values, err := s.cache.BitFieldRO(ctx, signKey(ctx, day), fmt.Sprintf("u%d", day.Day()), 0).Result()
	if err != nil {
		s.unavailable(c, "count consecutive sign-ins", err)
		return
	}
	count := min(day.Day(), bits.TrailingZeros64(^uint64(values[0])))
	httpx.Respond(c, http.StatusOK, "", count)
}
