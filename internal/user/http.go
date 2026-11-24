package user

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var (
	phonePattern = regexp.MustCompile(`^1[3-9][0-9]{9}$`)
	codePattern  = regexp.MustCompile(`^[0-9]{6}$`)
)

type loginRequest struct {
	Phone string `json:"phone"`
	Code  string `json:"code"`
}

type identityKey struct{}

type identity struct {
	profile profile
	token   string
}

// CurrentID reads the authenticated identity attached by RequireLogin.
func CurrentID(ctx context.Context) (uint64, bool) {
	current, ok := ctx.Value(identityKey{}).(identity)
	return current.profile.ID, ok
}

func (s *Service) Register(router *gin.Engine) {
	users := router.Group("/user")
	users.POST("/code", s.requestCode)
	users.POST("/login", s.createSession)

	protected := users.Group("", s.RequireLogin())
	protected.GET("/me", s.me)
	protected.GET("/:id", s.getProfile)
	protected.POST("/logout", s.logout)
	protected.POST("/sign", s.sign)
	protected.GET("/sign/count", s.signCount)
}

func (s *Service) requestCode(c *gin.Context) {
	phone := c.Query("phone")
	if !phonePattern.MatchString(phone) {
		httpx.Respond(c, http.StatusBadRequest, "invalid phone number", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	err := s.sendCode(ctx, phone)
	switch {
	case err == nil:
		httpx.Respond(c, http.StatusOK, "", nil)
	case errors.Is(err, errCodeCooldown):
		c.Header("Retry-After", strconv.FormatInt(int64(codeCooldown/time.Second), 10))
		httpx.Respond(c, http.StatusTooManyRequests, err.Error(), nil)
	case errors.Is(err, errCodeDeliveryDisabled):
		httpx.Respond(c, http.StatusServiceUnavailable, err.Error(), nil)
	default:
		s.unavailable(c, "issue verification code", err)
	}
}

func (s *Service) createSession(c *gin.Context) {
	var request loginRequest
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	if err := c.ShouldBindJSON(&request); err != nil ||
		!phonePattern.MatchString(request.Phone) || !codePattern.MatchString(request.Code) {
		httpx.Respond(c, http.StatusBadRequest, "phone and six-digit code are required", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	token, err := s.login(ctx, request.Phone, request.Code)
	switch {
	case err == nil:
		c.Header("Cache-Control", "no-store")
		httpx.Respond(c, http.StatusOK, "", token)
	case errors.Is(err, errInvalidCode):
		httpx.Respond(c, http.StatusUnauthorized, err.Error(), nil)
	default:
		s.unavailable(c, "create session", err)
	}
}

func (s *Service) RequireLogin() gin.HandlerFunc {
	return s.authenticate(true)
}

func (s *Service) OptionalLogin() gin.HandlerFunc {
	return s.authenticate(false)
}

// VisitorLogin attaches an identity when possible and keeps the public page accessible.
func (s *Service) VisitorLogin(c *gin.Context) {
	token := authorizationToken(c.GetHeader("Authorization"))
	if token != "" && len(token) <= 256 {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 25*time.Millisecond)
		values, err := readSession.Run(ctx, s.cache, []string{sessionKey(token)},
			sessionRenew.Milliseconds(), sessionTTL.Milliseconds()).StringSlice()
		cancel()
		if err == nil && len(values) == 3 {
			if id, err := strconv.ParseUint(values[0], 10, 64); err == nil && id != 0 {
				c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), identityKey{}, identity{profile: profile{ID: id}}))
			}
		}
	}
	c.Next()
}

func (s *Service) authenticate(required bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !required && c.GetHeader("Authorization") == "" {
			c.Next()
			return
		}
		token := authorizationToken(c.GetHeader("Authorization"))
		if token == "" || len(token) > 256 {
			rejectSession(c)
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		values, err := readSession.Run(ctx, s.cache, []string{sessionKey(token)},
			sessionRenew.Milliseconds(), sessionTTL.Milliseconds()).StringSlice()
		cancel()
		if err != nil {
			c.Abort()
			s.unavailable(c, "read session", err)
			return
		}
		if len(values) != 3 {
			rejectSession(c)
			return
		}
		id, err := strconv.ParseUint(values[0], 10, 64)
		if err != nil || id == 0 {
			rejectSession(c)
			return
		}
		current := identity{profile: profile{ID: id, NickName: values[1], Icon: values[2]}, token: token}
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), identityKey{}, current))
		c.Next()
	}
}

func authorizationToken(header string) string {
	header = strings.TrimSpace(header)
	if scheme, credential, ok := strings.Cut(header, " "); ok && strings.EqualFold(scheme, "Bearer") {
		return strings.TrimSpace(credential)
	}
	return header
}

func rejectSession(c *gin.Context) {
	c.Abort()
	httpx.Respond(c, http.StatusUnauthorized, "valid session required", nil)
}

func (s *Service) me(c *gin.Context) {
	current := c.Request.Context().Value(identityKey{}).(identity)
	c.Header("Cache-Control", "no-store")
	httpx.Respond(c, http.StatusOK, "", current.profile)
}

func (s *Service) getProfile(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		httpx.Respond(c, http.StatusBadRequest, "invalid user ID", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	result, err := s.accounts.lookup(ctx, "id = ?", id)
	switch {
	case err == nil:
		httpx.Respond(c, http.StatusOK, "", result)
	case errors.Is(err, gorm.ErrRecordNotFound):
		httpx.Respond(c, http.StatusOK, "", nil)
	default:
		s.unavailable(c, "look up profile", err)
	}
}

func (s *Service) logout(c *gin.Context) {
	current := c.Request.Context().Value(identityKey{}).(identity)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	if err := s.cache.Del(ctx, sessionKey(current.token)).Err(); err != nil {
		s.unavailable(c, "delete session", err)
		return
	}
	httpx.Respond(c, http.StatusOK, "", nil)
}

func (s *Service) unavailable(c *gin.Context, operation string, err error) {
	s.logger.Warn("user request failed", "operation", operation, "error", err)
	httpx.Respond(c, http.StatusServiceUnavailable, "user service temporarily unavailable", nil)
}
