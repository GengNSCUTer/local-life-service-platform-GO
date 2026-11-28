package seckill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mathrand "math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/observe"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

var orderIDPattern = regexp.MustCompile(`^(V2-(0[1-9]|1[0-6])-)?[A-Z2-7]{26}$`)

type Layout struct {
	Sharded      bool
	StockBuckets int
	Cluster      *redis.ClusterClient
}

type Service struct {
	db             *gorm.DB
	redis          *redis.Client
	shardRedis     *redis.ClusterClient
	layout         Layout
	queueShards    atomic.Pointer[[]redisShard]
	logger         *slog.Logger
	pendingTimeout time.Duration
	outboxLimit    int
	pendingLimit   int
	metrics        *metrics
}

func New(db *gorm.DB, cache *redis.Client, logger *slog.Logger, pendingTimeout time.Duration, outboxLimit, pendingLimit int, layout Layout) *Service {
	if pendingTimeout < time.Second {
		pendingTimeout = 15 * time.Minute
	}
	return &Service{db: db, redis: cache, shardRedis: layout.Cluster, layout: layout, logger: logger, pendingTimeout: pendingTimeout, outboxLimit: outboxLimit, pendingLimit: pendingLimit}
}

func (s *Service) Register(router *gin.Engine, requireLogin, submitLimit gin.HandlerFunc) {
	orders := router.Group("/voucher-order", requireLogin)
	orders.POST("/seckill/:id", submitLimit, s.submit)
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
	started := time.Now()
	result, err := s.reserve(ctx, voucherID, userID)
	if err != nil {
		if s.shardRedis != nil && isReservationRoutingError(err) {
			c.Header("Retry-After", "1")
		}
		s.observe(started, "redis_reserve", "error")
		s.unavailable(c, "reserve stock", err)
		return
	}
	if len(result) != 2 {
		s.observe(started, "redis_reserve", "error")
		s.unavailable(c, "reserve stock", errors.New("unexpected Lua response length"))
		return
	}
	data := gin.H{"orderId": result[1]}
	outcome := result[0]
	switch outcome {
	case "accepted", "duplicate", "missing", "closed", "sold_out", "overloaded", "pending_overloaded":
	default:
		outcome = "error"
	}
	s.observe(started, "redis_reserve", outcome)
	switch result[0] {
	case "accepted":
		httpx.Respond(c, http.StatusAccepted, "", data)
	case "duplicate":
		httpx.Respond(c, http.StatusConflict, "already submitted", data)
	case "missing":
		httpx.Respond(c, http.StatusNotFound, "activity unavailable", nil)
	case "closed":
		httpx.Respond(c, http.StatusConflict, "activity is outside its sale window", nil)
	case "overloaded", "pending_overloaded":
		c.Header("Retry-After", "1")
		httpx.Respond(c, http.StatusServiceUnavailable, "seckill temporarily overloaded", nil)
	case "sold_out":
		httpx.Respond(c, http.StatusConflict, "sold out", nil)
	default:
		s.unavailable(c, "reserve stock", fmt.Errorf("unknown Lua result %q", result[0]))
	}
}

func (s *Service) reserve(ctx context.Context, voucherID, userID uint64) (_ []string, reserveErr error) {
	ctx, span := observe.StartSpan(ctx, "redis.reserve", trace.SpanKindClient)
	defer func() {
		if reserveErr != nil {
			span.SetStatus(codes.Error, "Redis reservation failed")
		}
		span.End()
	}()
	headers := observe.TraceHeaders(ctx)
	shards := [2]redisShard{0, activityShard(voucherID)}
	if s.layout.Sharded {
		shards[0], shards[1] = shards[1], shards[0]
	}
	var result []string
	for _, shard := range shards {
		event := orderEvent{Version: 1, ID: shard.newOrderID(), VoucherID: voucherID, UserID: userID, CreatedAt: time.Now().UTC(), TraceHeaders: headers}
		payload, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		key := shard.activity(voucherID)
		keys := []string{key, key + ":buyers", orderKey(event.ID), shard.outbox(), shard.timeouts(), reservationBackupKey(event.ID)}
		args := []any{strconv.FormatUint(userID, 10), event.ID, payload, s.pendingTimeout.Milliseconds(), s.outboxLimit, s.pendingLimit}
		client := s.redisFor(shard)
		result, err = reserveStock.Run(ctx, client, keys, args...).StringSlice()
		// Retry only cluster replies that reject the script before Lua executes.
		for delay := 100 * time.Millisecond; shard != 0 && s.shardRedis != nil && isReservationRoutingError(err) && delay <= 400*time.Millisecond; delay *= 2 {
			if !wait(ctx, delay+mathrand.N(delay/2)) {
				return nil, ctx.Err()
			}
			result, err = reserveStock.Run(ctx, client, keys, args...).StringSlice()
		}
		if err != nil || len(result) != 2 || result[0] != "missing" {
			return result, err
		}
	}
	return result, nil
}

func isReservationRoutingError(err error) bool {
	if redis.IsTryAgainError(err) {
		return true
	}
	_, ask := redis.IsAskError(err)
	_, moved := redis.IsMovedError(err)
	return ask || moved
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

	order, err := s.findOrder(ctx, id, userID)
	switch {
	case errors.Is(err, errOrderNotFound):
		httpx.Respond(c, http.StatusNotFound, "order not found", nil)
	case err != nil:
		s.unavailable(c, "look up order", err)
	default:
		httpx.Respond(c, http.StatusOK, "", order)
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
	existing, err := s.findOrder(ctx, id, userID)
	if errors.Is(err, errOrderNotFound) {
		httpx.Respond(c, http.StatusNotFound, "order not found", nil)
		return
	}
	if err != nil {
		s.unavailable(c, "load order for cancellation", err)
		return
	}

	event := existing.event()
	order, err := s.cancelOrder(ctx, event, reasonUserCancelled)
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
	_, err = s.syncResolution(syncCtx, event)
	syncStop()
	result := cancellationResult{Order: order, CompensationPending: err != nil}
	if err != nil {
		s.logger.Warn("cancelled order is waiting for Redis compensation", "order_id", id, "error", err)
		httpx.Respond(c, http.StatusAccepted, "", result)
		return
	}
	httpx.Respond(c, http.StatusOK, "", result)
}

func (s *Service) findOrder(ctx context.Context, id string, userID uint64) (Order, error) {
	order, err := loadOrder(ctx, s.db, id)
	if errors.Is(err, errOrderNotFound) {
		event, _, reservationErr := s.reservation(ctx, id)
		switch {
		case reservationErr == nil:
			order, err = event.order(statusPending), nil
		case errors.Is(reservationErr, errUnreadableReservation):
			// Confirmation or cancellation may have committed and removed the Redis payload.
			order, err = loadOrder(ctx, s.db, id)
		default:
			return Order{}, reservationErr
		}
	}
	if err == nil && order.UserID != userID {
		return Order{}, errOrderNotFound
	}
	return order, err
}

func (s *Service) unavailable(c *gin.Context, operation string, err error) {
	s.logger.Warn("seckill request failed", "operation", operation, "error", err)
	httpx.Respond(c, http.StatusServiceUnavailable, "seckill temporarily unavailable", nil)
}
