package observe

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

func HTTP(reg prometheus.Registerer) gin.HandlerFunc {
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "hmdp_http_request_duration_seconds",
		Help:    "HTTP request latency and count by method, route template and status.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"method", "route", "status"})
	reg.MustRegister(latency)
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route, method := c.FullPath(), c.Request.Method
		if route == "" {
			route = "unmatched"
		}
		switch method {
		case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodHead, http.MethodOptions:
		default:
			method = "other"
		}
		latency.WithLabelValues(method, route, strconv.Itoa(c.Writer.Status())).Observe(time.Since(start).Seconds())
	}
}
