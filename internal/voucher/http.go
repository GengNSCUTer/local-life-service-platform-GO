package voucher

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/seckill"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var keyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type writeRequest struct {
	ShopID      uint64    `json:"shopId" binding:"required"`
	Title       string    `json:"title" binding:"required,max=255"`
	SubTitle    *string   `json:"subTitle" binding:"omitempty,max=255"`
	Rules       *string   `json:"rules" binding:"omitempty,max=1024"`
	PayValue    uint64    `json:"payValue"`
	ActualValue int64     `json:"actualValue" binding:"gte=0"`
	Stock       uint32    `json:"stock"`
	BeginTime   time.Time `json:"beginTime"`
	EndTime     time.Time `json:"endTime"`
}

func (s *Service) Register(router *gin.Engine, requireLogin gin.HandlerFunc) {
	vouchers := router.Group("/voucher")
	vouchers.GET("/list/:shopId", s.listByShop)
	maintenance := vouchers.Group("", requireLogin, s.requireAdmin)
	maintenance.POST("", s.createHandler(normalVoucher))
	maintenance.POST("/seckill", s.createHandler(seckillVoucher))
	maintenance.POST("/seckill/:id/initialize", s.initializeHandler)
}

func (s *Service) requireAdmin(c *gin.Context) {
	id, ok := user.CurrentID(c.Request.Context())
	if !ok || s.adminID == 0 || id != s.adminID {
		c.Abort()
		httpx.Respond(c, http.StatusForbidden, "voucher administrator required", nil)
		return
	}
	c.Next()
}

func (s *Service) createHandler(kind uint8) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request writeRequest
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16*1024)
		if err := c.ShouldBindJSON(&request); err != nil || strings.TrimSpace(request.Title) == "" {
			httpx.Respond(c, http.StatusBadRequest, "invalid voucher fields", nil)
			return
		}
		key := c.GetHeader("Idempotency-Key")
		if key != "" && !keyPattern.MatchString(key) {
			httpx.Respond(c, http.StatusBadRequest, "invalid idempotency key", nil)
			return
		}
		var activity *seckill.Activity
		if kind == seckillVoucher {
			config, err := activityFor(request.Stock, request.BeginTime, request.EndTime)
			if err != nil {
				httpx.Respond(c, http.StatusBadRequest, err.Error(), nil)
				return
			}
			activity = &config
		}
		item := Voucher{
			ShopID: request.ShopID, Title: strings.TrimSpace(request.Title), SubTitle: request.SubTitle,
			Rules: request.Rules, PayValue: request.PayValue, ActualValue: request.ActualValue, Type: kind, Status: activeStatus,
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		id, err := s.create(ctx, item, activity, key)
		if err != nil {
			s.respond(c, "save voucher", err, nil)
			return
		}
		if activity != nil {
			if err := s.initialize(ctx, id); err != nil {
				s.respond(c, "initialize voucher stock", err, id)
				return
			}
		}
		httpx.Respond(c, http.StatusOK, "", id)
	}
}

func (s *Service) initializeHandler(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		httpx.Respond(c, http.StatusBadRequest, "invalid voucher ID", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	if err := s.initialize(ctx, id); err != nil {
		s.respond(c, "initialize voucher stock", err, id)
		return
	}
	httpx.Respond(c, http.StatusOK, "", id)
}

func (s *Service) listByShop(c *gin.Context) {
	shopID, err := strconv.ParseUint(c.Param("shopId"), 10, 64)
	if err != nil || shopID == 0 {
		httpx.Respond(c, http.StatusBadRequest, "invalid shop ID", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	items, err := s.repo.listByShop(ctx, shopID)
	if err != nil {
		s.respond(c, "list vouchers", err, nil)
		return
	}
	httpx.Respond(c, http.StatusOK, "", items)
}

func (s *Service) respond(c *gin.Context, operation string, err error, data any) {
	switch {
	case errors.Is(err, errInvalidWindow):
		httpx.Respond(c, http.StatusBadRequest, err.Error(), data)
	case errors.Is(err, errUnknownShop), errors.Is(err, gorm.ErrRecordNotFound):
		httpx.Respond(c, http.StatusNotFound, "shop or voucher activity not found", nil)
	case errors.Is(err, errKeyConflict), errors.Is(err, seckill.ErrStockWindowStarted):
		httpx.Respond(c, http.StatusConflict, err.Error(), data)
	default:
		s.logger.Warn("voucher request failed", "operation", operation, "voucher_id", data, "error", err)
		httpx.Respond(c, http.StatusServiceUnavailable, "voucher service temporarily unavailable", data)
	}
}
