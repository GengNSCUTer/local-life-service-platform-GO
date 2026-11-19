package app

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
)

type healthProbe struct {
	name  string
	check func(context.Context) error
}

func newRouter(logger *slog.Logger, probes []healthProbe, middleware ...gin.HandlerFunc) *gin.Engine {
	router := gin.New()
	router.Use(middleware...)
	router.Use(httpx.RequestID, httpx.AccessLog(logger), httpx.Recovery(logger))
	router.HandleMethodNotAllowed = true
	router.NoRoute(func(c *gin.Context) {
		httpx.Respond(c, http.StatusNotFound, "route not found", nil)
	})
	router.NoMethod(func(c *gin.Context) {
		httpx.Respond(c, http.StatusMethodNotAllowed, "method not allowed", nil)
	})

	health := router.Group("/health")
	health.GET("/live", func(c *gin.Context) {
		httpx.Respond(c, http.StatusOK, "", gin.H{"status": "ok"})
	})
	health.GET("/ready", readinessHandler(probes))
	return router
}

func readinessHandler(probes []healthProbe) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()

		results := make([]bool, len(probes))
		var group sync.WaitGroup
		for i, probe := range probes {
			group.Go(func() {
				results[i] = probe.check(ctx) == nil
			})
		}
		group.Wait()

		status := http.StatusOK
		var message string
		checks := make(map[string]string, len(probes))
		for i, probe := range probes {
			if results[i] {
				checks[probe.name] = "up"
				continue
			}
			checks[probe.name] = "down"
			status = http.StatusServiceUnavailable
			message = "dependencies unavailable"
		}
		httpx.Respond(c, status, message, gin.H{"checks": checks})
	}
}
