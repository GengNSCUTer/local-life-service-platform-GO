package seckill

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var orderIDPattern = regexp.MustCompile(`^[A-Z2-7]{26}$`)

type Service struct {
	db     *gorm.DB
	redis  *redis.Client
	logger *slog.Logger
}

func New(db *gorm.DB, cache *redis.Client, logger *slog.Logger) *Service {
	return &Service{db: db, redis: cache, logger: logger}
}

func (s *Service) Register(router *gin.Engine) {
	orders := router.Group("/voucher-order")
	orders.POST("/seckill/:id", s.submit)
	orders.GET("/:id", s.status)
}

func (s *Service) submit(c *gin.Context) {
	voucherID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || voucherID == 0 {
		httpx.Respond(c, http.StatusBadRequest, "invalid voucher ID", nil)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	userID, ok := s.sessionUser(ctx, c)
	if !ok {
		return
	}
	event := orderEvent{Version: 1, ID: rand.Text(), VoucherID: voucherID, UserID: userID, CreatedAt: time.Now().UTC()}
	payload, err := json.Marshal(event)
	if err != nil {
		s.unavailable(c, "encode order event", err)
		return
	}
	key := activityKey(voucherID)
	result, err := reserveStock.Run(ctx, s.redis, []string{key, key + ":buyers", orderKey(event.ID), outboxKey},
		strconv.FormatUint(userID, 10), event.ID, payload).StringSlice()
	if err != nil {
		s.unavailable(c, "reserve stock", err)
		return
	}
	if len(result) != 2 {
		s.unavailable(c, "reserve stock", errors.New("unexpected Lua response length"))
		return
	}
	data := gin.H{"orderId": result[1]}
	switch result[0] {
	case "accepted":
		httpx.Respond(c, http.StatusAccepted, "", data)
	case "duplicate":
		httpx.Respond(c, http.StatusConflict, "already submitted", data)
	case "missing":
		httpx.Respond(c, http.StatusNotFound, "activity unavailable", nil)
	case "closed":
		httpx.Respond(c, http.StatusConflict, "activity is outside its sale window", nil)
	case "sold_out":
		httpx.Respond(c, http.StatusConflict, "sold out", nil)
	default:
		s.unavailable(c, "reserve stock", fmt.Errorf("unknown Lua result %q", result[0]))
	}
}

func (s *Service) status(c *gin.Context) {
	id := c.Param("id")
	if !orderIDPattern.MatchString(id) {
		httpx.Respond(c, http.StatusBadRequest, "invalid order ID", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	userID, ok := s.sessionUser(ctx, c)
	if !ok {
		return
	}

	var order Order
	err := s.db.WithContext(ctx).Table("seckill_orders").Take(&order, "id = ?", id).Error
	switch {
	case err == nil:
		if order.UserID != userID {
			httpx.Respond(c, http.StatusNotFound, "order not found", nil)
			return
		}
		httpx.Respond(c, http.StatusOK, "", order)
	case errors.Is(err, gorm.ErrRecordNotFound):
		payload, err := s.redis.Get(ctx, orderKey(id)).Bytes()
		if errors.Is(err, redis.Nil) {
			httpx.Respond(c, http.StatusNotFound, "order not found", nil)
			return
		}
		if err != nil {
			s.unavailable(c, "look up reservation", err)
			return
		}
		var event orderEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			s.unavailable(c, "decode reservation", err)
			return
		}
		if event.UserID != userID {
			httpx.Respond(c, http.StatusNotFound, "order not found", nil)
			return
		}
		httpx.Respond(c, http.StatusOK, "", event.order(statusPending))
	default:
		s.unavailable(c, "look up order", err)
	}
}

func (s *Service) sessionUser(ctx context.Context, c *gin.Context) (uint64, bool) {
	token := strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
	if token == "" || len(token) > 256 {
		httpx.Respond(c, http.StatusUnauthorized, "valid session required", nil)
		return 0, false
	}
	value, err := s.redis.HGet(ctx, "login:token:"+token, "id").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		s.unavailable(c, "read session", err)
		return 0, false
	}
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 {
		httpx.Respond(c, http.StatusUnauthorized, "valid session required", nil)
		return 0, false
	}
	return id, true
}

func (s *Service) unavailable(c *gin.Context, operation string, err error) {
	s.logger.Warn("seckill request failed", "operation", operation, "error", err)
	httpx.Respond(c, http.StatusServiceUnavailable, "seckill temporarily unavailable", nil)
}
