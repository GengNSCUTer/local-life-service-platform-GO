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
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var orderIDPattern = regexp.MustCompile(`^[A-Z2-7]{26}$`)

type Service struct {
	db             *gorm.DB
	redis          *redis.Client
	logger         *slog.Logger
	pendingTimeout time.Duration
}

func New(db *gorm.DB, cache *redis.Client, logger *slog.Logger, pendingTimeout ...time.Duration) *Service {
	timeout := 15 * time.Minute
	if len(pendingTimeout) > 0 && pendingTimeout[0] >= time.Second {
		timeout = pendingTimeout[0]
	}
	return &Service{db: db, redis: cache, logger: logger, pendingTimeout: timeout}
}

func (s *Service) Register(router *gin.Engine, requireLogin gin.HandlerFunc) {
	orders := router.Group("/voucher-order", requireLogin)
	orders.POST("/seckill/:id", s.submit)
	orders.GET("/:id", s.status)
	orders.GET("", s.listOrders)
	orders.POST("/:id/cancel", s.cancel)
}

func (s *Service) submit(c *gin.Context) {
	voucherID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || voucherID == 0 {
		httpx.Respond(c, http.StatusBadRequest, "invalid voucher ID", nil)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	userID, _ := user.CurrentID(ctx)
	event := orderEvent{Version: 1, ID: rand.Text(), VoucherID: voucherID, UserID: userID, CreatedAt: time.Now().UTC()}
	payload, err := json.Marshal(event)
	if err != nil {
		s.unavailable(c, "encode order event", err)
		return
	}
	key := activityKey(voucherID)
	result, err := reserveStock.Run(ctx, s.redis,
		[]string{key, key + ":buyers", orderKey(event.ID), outboxKey, timeoutKey},
		strconv.FormatUint(userID, 10), event.ID, payload, s.pendingTimeout.Milliseconds()).StringSlice()
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
	userID, _ := user.CurrentID(ctx)

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

func (s *Service) listOrders(c *gin.Context) {
	page, err := strconv.Atoi(c.DefaultQuery("current", "1"))
	if err != nil || page < 1 || page > 10000 {
		httpx.Respond(c, http.StatusBadRequest, "page must be between 1 and 10000", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	userID, _ := user.CurrentID(ctx)
	const pageSize = 10
	orders := make([]Order, 0)
	err = s.db.WithContext(ctx).Table("seckill_orders").Where("user_id = ?", userID).
		Order("created_at DESC, id DESC").Offset((page - 1) * pageSize).Limit(pageSize).Find(&orders).Error
	if err != nil {
		s.unavailable(c, "list orders", err)
		return
	}
	httpx.Respond(c, http.StatusOK, "", orders)
}

func (s *Service) cancel(c *gin.Context) {
	id := c.Param("id")
	if !orderIDPattern.MatchString(id) {
		httpx.Respond(c, http.StatusBadRequest, "invalid order ID", nil)
		return
	}

	ctx, stop := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer stop()
	userID, _ := user.CurrentID(ctx)
	event, err := s.findOrderEvent(ctx, id, userID)
	if errors.Is(err, errOrderNotFound) {
		httpx.Respond(c, http.StatusNotFound, "order not found", nil)
		return
	}
	if err != nil {
		s.unavailable(c, "load order for cancellation", err)
		return
	}

	order, err := s.cancelOrder(ctx, event, "user_cancelled")
	switch {
	case errors.Is(err, errOrderNotFound):
		httpx.Respond(c, http.StatusNotFound, "order not found", nil)
		return
	case errors.Is(err, errOrderState):
		httpx.Respond(c, http.StatusConflict, "order cannot be cancelled in its current state", nil)
		return
	case err != nil:
		s.unavailable(c, "cancel order", err)
		return
	}

	syncCtx, syncStop := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 2*time.Second)
	err = s.syncResolution(syncCtx, event)
	syncStop()
	result := cancellationResult{Order: order, CompensationPending: err != nil}
	if err != nil {
		s.logger.Warn("cancelled order is waiting for Redis compensation", "order_id", id, "error", err)
		httpx.Respond(c, http.StatusAccepted, "", result)
		return
	}
	httpx.Respond(c, http.StatusOK, "", result)
}

func (s *Service) findOrderEvent(ctx context.Context, id string, userID uint64) (orderEvent, error) {
	var order Order
	err := s.db.WithContext(ctx).Table("seckill_orders").Take(&order, "id = ?", id).Error
	if err == nil {
		if order.UserID != userID {
			return orderEvent{}, errOrderNotFound
		}
		return orderEvent{
			Version: 1, ID: order.ID, VoucherID: order.VoucherID,
			UserID: order.UserID, CreatedAt: order.CreatedAt,
		}, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return orderEvent{}, err
	}

	payload, err := s.redis.Get(ctx, orderKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return orderEvent{}, errOrderNotFound
	}
	if err != nil {
		return orderEvent{}, err
	}
	var event orderEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return orderEvent{}, fmt.Errorf("decode reservation: %w", err)
	}
	if event.ID != id || event.UserID != userID {
		return orderEvent{}, errOrderNotFound
	}
	return event, nil
}

func (s *Service) unavailable(c *gin.Context, operation string, err error) {
	s.logger.Warn("seckill request failed", "operation", operation, "error", err)
	httpx.Respond(c, http.StatusServiceUnavailable, "seckill temporarily unavailable", nil)
}
