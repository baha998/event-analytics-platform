package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"devops-fall2025-23b031152/pkg/metrics"
)

// PrometheusMiddleware tracks HTTP metrics
func PrometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Track active connections
		metrics.ActiveConnections.Inc()
		defer metrics.ActiveConnections.Dec()

		start := time.Now()

		c.Next()

		duration := time.Since(start).Seconds()
		status := c.Writer.Status()
		method := c.Request.Method
		endpoint := c.FullPath()

		if endpoint == "" {
			endpoint = c.Request.URL.Path
		}

		statusStr := strconv.Itoa(status)
		metrics.HTTPRequestsTotal.WithLabelValues(method, endpoint, statusStr).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method, endpoint).Observe(duration)
	}
}
