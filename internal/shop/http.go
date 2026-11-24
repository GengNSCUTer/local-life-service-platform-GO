package shop

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const requestTimeout = 3 * time.Second

// Pointer fields distinguish an omitted field from an explicit empty string or zero.
// Operational fields such as sold, score, and comments are outside the write DTO.
type shopPatch struct {
	Name      *string  `json:"name" binding:"omitempty,min=1,max=128"`
	TypeID    *uint64  `json:"typeId" binding:"omitempty,gt=0"`
	Images    *string  `json:"images" binding:"omitempty,max=1024"`
	Area      *string  `json:"area" binding:"omitempty,max=128"`
	Address   *string  `json:"address" binding:"omitempty,min=1,max=255"`
	X         *float64 `json:"x" binding:"omitempty,gte=-180,lte=180"`
	Y         *float64 `json:"y" binding:"omitempty,gte=-85.05112878,lte=85.05112878"`
	AvgPrice  *uint64  `json:"avgPrice"`
	OpenHours *string  `json:"openHours" binding:"omitempty,max=32"`
}

type shopWriteRequest struct {
	ID uint64 `json:"id"`
	shopPatch
}

func (s *Service) Register(router *gin.Engine, requireLogin, visitorLogin gin.HandlerFunc) {
	router.GET("/shop-type/list", s.types)
	shops := router.Group("/shop")
	shops.GET("/:id", visitorLogin, s.get)
	shops.GET("/statistics/uv", requireLogin, s.requireAdmin, s.dailyUV)
	shops.GET("/of/type", s.listByType)
	shops.GET("/of/name", s.listByName)
	maintenance := shops.Group("", requireLogin, s.requireAdmin)
	maintenance.POST("", s.create)
	maintenance.PUT("", s.update)
}

func (s *Service) requireAdmin(c *gin.Context) {
	id, ok := user.CurrentID(c.Request.Context())
	if !ok || s.adminID == 0 || id != s.adminID {
		c.Abort()
		httpx.Respond(c, http.StatusForbidden, "shop administrator required", nil)
		return
	}
	c.Next()
}

func (s *Service) get(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		httpx.Respond(c, http.StatusBadRequest, "invalid shop ID", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), requestTimeout)
	defer cancel()
	item, err := s.detail(ctx, id)
	if err != nil {
		s.respondError(c, "get shop", err)
		return
	}
	s.recordVisit(c)
	httpx.Respond(c, http.StatusOK, "", item)
}

func (s *Service) types(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), requestTimeout)
	defer cancel()
	items, err := s.shops.types(ctx)
	if err != nil {
		s.respondError(c, "list shop types", err)
		return
	}
	httpx.Respond(c, http.StatusOK, "", items)
}

func (s *Service) listByType(c *gin.Context) {
	typeID, err := strconv.ParseUint(c.Query("typeId"), 10, 64)
	if err != nil || typeID == 0 {
		httpx.Respond(c, http.StatusBadRequest, "invalid shop type ID", nil)
		return
	}
	x, hasX := c.GetQuery("x")
	y, hasY := c.GetQuery("y")
	if hasX || hasY {
		if _, cursor := c.GetQuery("afterId"); cursor {
			httpx.Respond(c, http.StatusBadRequest, "nearby shops use current", nil)
			return
		}
		s.nearby(c, typeID, x, y)
		return
	}
	s.list(c, typeID, "", 5)
}

func (s *Service) listByName(c *gin.Context) {
	name := strings.TrimSpace(c.Query("name"))
	if utf8.RuneCountInString(name) > 128 {
		httpx.Respond(c, http.StatusBadRequest, "shop name is too long", nil)
		return
	}
	s.list(c, 0, name, 10)
}

type shopPage struct {
	List        []shopRecord `json:"list"`
	NextAfterID string       `json:"nextAfterId,omitempty"`
}

func (s *Service) list(c *gin.Context, typeID uint64, name string, size int) {
	cursor, cursorMode := c.GetQuery("afterId")
	var afterID uint64
	var offset int
	limit := size
	if cursorMode {
		id, err := strconv.ParseUint(cursor, 10, 64)
		if _, hasCurrent := c.GetQuery("current"); hasCurrent || err != nil {
			httpx.Respond(c, http.StatusBadRequest, "use a valid afterId or current", nil)
			return
		}
		afterID = id
		limit++
	} else {
		page, err := strconv.Atoi(c.DefaultQuery("current", "1"))
		if err != nil || page < 1 || page > 10000 {
			httpx.Respond(c, http.StatusBadRequest, "page must be between 1 and 10000", nil)
			return
		}
		offset = (page - 1) * size
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), requestTimeout)
	defer cancel()
	items, err := s.shops.list(ctx, typeID, name, offset, limit, afterID)
	if err != nil {
		s.respondError(c, "list shops", err)
		return
	}
	if !cursorMode {
		httpx.Respond(c, http.StatusOK, "", items)
		return
	}
	result := shopPage{List: items}
	if len(items) > size {
		result.List = items[:size]
		result.NextAfterID = strconv.FormatUint(items[size-1].ID, 10)
	}
	httpx.Respond(c, http.StatusOK, "", result)
}

func bindShopWrite(c *gin.Context) (shopWriteRequest, bool) {
	var request shopWriteRequest
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16*1024)
	if err := c.ShouldBindJSON(&request); err != nil {
		httpx.Respond(c, http.StatusBadRequest, "invalid shop fields", nil)
		return request, false
	}
	for _, field := range []*string{request.Name, request.Address} {
		if field == nil {
			continue
		}
		*field = strings.TrimSpace(*field)
		if *field == "" {
			httpx.Respond(c, http.StatusBadRequest, "name and address must not be blank", nil)
			return request, false
		}
	}
	return request, true
}

func (s *Service) create(c *gin.Context) {
	request, ok := bindShopWrite(c)
	if !ok {
		return
	}
	if request.Name == nil || request.TypeID == nil || request.Address == nil || request.X == nil || request.Y == nil {
		httpx.Respond(c, http.StatusBadRequest, "name, typeId, address, x and y are required", nil)
		return
	}
	item := shopRecord{
		Name: *request.Name, TypeID: *request.TypeID, Address: *request.Address,
		X: *request.X, Y: *request.Y, Area: request.Area, AvgPrice: request.AvgPrice, OpenHours: request.OpenHours,
	}
	if request.Images != nil {
		item.Images = *request.Images
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), requestTimeout)
	defer cancel()
	if err := s.shops.create(ctx, &item); err != nil {
		s.respondError(c, "create shop", err)
		return
	}
	s.invalidate(ctx, item.ID)
	httpx.Respond(c, http.StatusOK, "", item.ID)
}

func (s *Service) update(c *gin.Context) {
	request, ok := bindShopWrite(c)
	if !ok {
		return
	}
	if request.ID == 0 || request.shopPatch == (shopPatch{}) {
		httpx.Respond(c, http.StatusBadRequest, "shop ID and at least one editable field are required", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), requestTimeout)
	defer cancel()
	if err := s.shops.update(ctx, request.ID, request.shopPatch); err != nil {
		s.respondError(c, "update shop", err)
		return
	}
	s.invalidate(ctx, request.ID)
	httpx.Respond(c, http.StatusOK, "", nil)
}

func (s *Service) respondError(c *gin.Context, operation string, err error) {
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		httpx.Respond(c, http.StatusNotFound, "shop not found", nil)
	case errors.Is(err, errUnknownType):
		httpx.Respond(c, http.StatusBadRequest, err.Error(), nil)
	default:
		s.logger.Warn("shop request failed", "operation", operation, "error", err)
		httpx.Respond(c, http.StatusServiceUnavailable, "shop service temporarily unavailable", nil)
	}
}
