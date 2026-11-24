package social

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/user"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func (s *Service) Register(router *gin.Engine, login, optionalLogin gin.HandlerFunc) {
	blogs := router.Group("/blog")
	blogs.POST("", s.limitWrites, login, s.create)
	blogs.GET("/:id", optionalLogin, s.detail)
	blogs.POST("/:id/comments", s.limitWrites, login, s.createComment)
	blogs.GET("/:id/comments", s.listComments)
	blogs.DELETE("/:id/comments/:commentId", s.limitWrites, login, s.deleteComment)
	blogs.GET("/hot", optionalLogin, s.hot)
	blogs.GET("/of/user", optionalLogin, s.list)
	blogs.GET("/of/me", login, s.list)
	blogs.GET("/of/follow", login, s.feed)
	blogs.GET("/notifications", login, s.notifications)
	blogs.GET("/likes/:id", s.likes)
	blogs.PUT("/like/:id", s.limitWrites, login, s.like)
	blogs.DELETE("/like/:id", s.limitWrites, login, s.like)
	follows := router.Group("/follow", login)
	follows.PUT("/:id/:state", s.limitWrites, s.follow)
	follows.GET("/or/not/:id", s.isFollowing)
	follows.GET("/common/:id", s.common)
}

func (s *Service) limitWrites(c *gin.Context) {
	if s.writeSlots == nil {
		c.Next()
		return
	}
	select {
	case s.writeSlots <- struct{}{}:
		defer func() { <-s.writeSlots }()
		c.Next()
	default:
		c.Abort()
		if s.metrics != nil {
			s.metrics.writeRejected.Inc()
		}
		c.Header("Retry-After", "1")
		httpx.Respond(c, http.StatusServiceUnavailable, "social writes temporarily overloaded", nil)
	}
}

func requestContext(c *gin.Context) (context.Context, context.CancelFunc, uint64) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	viewer, _ := user.CurrentID(ctx)
	return ctx, cancel, viewer
}

func positiveID(c *gin.Context, value string) (uint64, bool) {
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 {
		httpx.Respond(c, http.StatusBadRequest, "invalid ID", nil)
		return 0, false
	}
	return id, true
}

func (s *Service) respond(c *gin.Context, data any, err error) {
	switch {
	case err == nil:
		httpx.Respond(c, http.StatusOK, "", data)
	case errors.Is(err, gorm.ErrRecordNotFound), errors.Is(err, errInvalidShop):
		httpx.Respond(c, http.StatusNotFound, "blog or shop not found", nil)
	case errors.Is(err, errInvalidFollow):
		httpx.Respond(c, http.StatusBadRequest, err.Error(), nil)
	default:
		s.logger.Warn("social request failed", "route", c.FullPath(), "error", err)
		httpx.Respond(c, http.StatusServiceUnavailable, "social service temporarily unavailable", nil)
	}
}

func (s *Service) create(c *gin.Context) {
	var input blogInput
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 32*1024)
	if err := c.ShouldBindJSON(&input); err != nil || strings.TrimSpace(input.Title) == "" || strings.TrimSpace(input.Content) == "" {
		httpx.Respond(c, http.StatusBadRequest, "shopId, title and content are required", nil)
		return
	}
	input.Title = strings.TrimSpace(input.Title)
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	id, err := s.publish(ctx, viewer, input)
	s.respond(c, id, err)
}

func (s *Service) detail(c *gin.Context) {
	id, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	item, err := s.loadDetail(ctx, id, viewer)
	s.respond(c, item, err)
}

func (s *Service) list(c *gin.Context) {
	page, err := strconv.Atoi(c.DefaultQuery("current", "1"))
	if err != nil || page < 1 || page > 10000 {
		httpx.Respond(c, http.StatusBadRequest, "page must be between 1 and 10000", nil)
		return
	}
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	query := s.blogs(ctx, viewer)
	switch c.FullPath() {
	case "/blog/of/user":
		id, ok := positiveID(c, c.Query("id"))
		if !ok {
			return
		}
		query = query.Where("b.user_id = ?", id)
	case "/blog/of/me":
		query = query.Where("b.user_id = ?", viewer)
	}
	items := make([]blogView, 0)
	err = query.Order("b.id DESC").Offset((page - 1) * pageSize).Limit(pageSize).Find(&items).Error
	s.respond(c, items, err)
}

type feedPage struct {
	List       []blogView `json:"list"`
	NextCursor string     `json:"nextCursor,omitempty"`
	MinTime    uint64     `json:"minTime"`
	Offset     int        `json:"offset"`
}

func (s *Service) feed(c *gin.Context) {
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	var before uint64
	cursor := c.Query("cursor")
	if cursor == "" {
		cursor = c.Query("lastId")
		if cursor == "0" {
			cursor = ""
		}
	}
	if cursor != "" {
		id, ok := positiveID(c, cursor)
		if !ok {
			return
		}
		before = id
	}
	query := s.blogs(ctx, viewer)
	if s.boundedFeed {
		var ids []uint64
		condition := ""
		var args []any
		if before != 0 {
			condition = " AND candidate.id < ?"
			args = append(args, before)
		}
		args = append(args, viewer)
		err := s.db.WithContext(ctx).Raw(`SELECT recent.id FROM social_follows AS f
			JOIN LATERAL (SELECT candidate.id FROM tb_blog AS candidate FORCE INDEX (idx_blog_author)
			WHERE candidate.user_id = f.follow_user_id`+condition+` ORDER BY candidate.id DESC LIMIT 11) AS recent ON TRUE
			WHERE f.user_id = ? ORDER BY recent.id DESC LIMIT 11`, args...).Scan(&ids).Error
		if err != nil {
			s.respond(c, nil, err)
			return
		}
		if len(ids) == 0 {
			s.respond(c, feedPage{List: []blogView{}}, nil)
			return
		}
		query = query.Joins("JOIN social_follows f ON f.follow_user_id = b.user_id AND f.user_id = ?", viewer).Where("b.id IN ?", ids)
	} else {
		// Materialize a small page of IDs before reading blog bodies and viewer details.
		// STRAIGHT_JOIN walks the blog primary index in order and probes the follow index.
		page := s.db.WithContext(ctx).Table("tb_blog AS candidate FORCE INDEX (PRIMARY)").
			Select("STRAIGHT_JOIN candidate.id").
			Joins("JOIN social_follows candidate_follow ON candidate_follow.follow_user_id = candidate.user_id AND candidate_follow.user_id = ?", viewer).
			Where("EXISTS (SELECT 1 FROM social_follows available WHERE available.user_id = ?)", viewer).
			Order("candidate.id DESC").Limit(pageSize + 1)
		if before != 0 {
			page = page.Where("candidate.id < ?", before)
		}
		query = query.Joins("JOIN (?) feed_page ON feed_page.id = b.id", page)
	}
	page := feedPage{List: make([]blogView, 0)}
	err := query.Order("b.id DESC").Limit(pageSize + 1).Find(&page.List).Error
	if len(page.List) > pageSize {
		page.List = page.List[:pageSize]
		page.NextCursor = strconv.FormatUint(page.List[pageSize-1].ID, 10)
	}
	if len(page.List) > 0 {
		page.MinTime = page.List[len(page.List)-1].ID
	}
	s.respond(c, page, err)
}

func (s *Service) like(c *gin.Context) {
	id, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	state := c.Query("liked")
	if c.Request.Method == http.MethodPut && state != "true" && state != "false" {
		httpx.Respond(c, http.StatusBadRequest, "liked must be true or false", nil)
		return
	}
	desired := c.Request.Method == http.MethodPut && state == "true"
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	s.respond(c, nil, s.changeLike(ctx, id, viewer, desired))
}

func (s *Service) likes(c *gin.Context) {
	id, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	ctx, cancel, _ := requestContext(c)
	defer cancel()
	items := make([]profile, 0)
	err := s.db.WithContext(ctx).Table("social_blog_likes l").
		Select("u.id, COALESCE(u.nick_name, '') AS nick_name, COALESCE(u.icon, '') AS icon").
		Joins("JOIN tb_user u ON u.id = l.user_id").Where("l.blog_id = ?", id).
		Order("l.created_at DESC, l.user_id DESC").Limit(5).Find(&items).Error
	s.respond(c, items, err)
}

func (s *Service) follow(c *gin.Context) {
	id, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	state := c.Param("state")
	if state != "true" && state != "false" {
		httpx.Respond(c, http.StatusBadRequest, "follow state must be true or false", nil)
		return
	}
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	s.respond(c, nil, s.changeFollow(ctx, viewer, id, state == "true"))
}

func (s *Service) isFollowing(c *gin.Context) {
	id, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	var count int64
	err := s.db.WithContext(ctx).Table("social_follows").Where("user_id = ? AND follow_user_id = ?", viewer, id).Count(&count).Error
	s.respond(c, count > 0, err)
}

func (s *Service) common(c *gin.Context) {
	id, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	items, err := s.commonFollows(ctx, viewer, id)
	s.respond(c, items, err)
}
