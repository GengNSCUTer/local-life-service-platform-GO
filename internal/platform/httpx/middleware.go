package httpx

import (
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"
)

const requestIDHeader = "X-Request-ID"

func RequestID(c *gin.Context) {
	c.Header(requestIDHeader, rand.Text())
	c.Next()
}

func AccessLog(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.Info("http request",
			"request_id", c.Writer.Header().Get(requestIDHeader),
			"method", c.Request.Method,
			"route", c.FullPath(),
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
}

func Recovery(logger *slog.Logger) gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, _ any) {
		logger.Error("http handler panic",
			"request_id", c.Writer.Header().Get(requestIDHeader),
			"stack", string(debug.Stack()),
		)
		c.Abort()
		if !c.Writer.Written() {
			Respond(c, http.StatusInternalServerError, "internal server error", nil)
		}
	})
}
