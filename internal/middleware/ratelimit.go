package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

type ipRateLimiter struct {
	visitors map[string]*rate.Limiter
	mu       sync.Mutex
	limit    rate.Limit
	burst    int
}

func newIPRateLimiter(limit rate.Limit, burst int) *ipRateLimiter {
	return &ipRateLimiter{
		visitors: make(map[string]*rate.Limiter),
		limit:    limit,
		burst:    burst,
	}
}

func (l *ipRateLimiter) getLimiter(ip string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()

	limiter, exists := l.visitors[ip]
	if !exists {
		limiter = rate.NewLimiter(l.limit, l.burst)
		l.visitors[ip] = limiter
	}

	return limiter
}

// RateLimitMiddleware limits requests per IP.
func RateLimitMiddleware(limit rate.Limit, burst int, log zerolog.Logger) gin.HandlerFunc {
	ipLimiter := newIPRateLimiter(limit, burst)

	return func(c *gin.Context) {
		ip := c.ClientIP()
		limiter := ipLimiter.getLimiter(ip)

		if !limiter.Allow() {
			log.Warn().Str("ip", ip).Msg("rate limit exceeded")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": gin.H{
					"code":    "rate_limit_exceeded",
					"message": "Too many requests. Try again later.",
				},
			})
			return
		}

		c.Next()
	}
}

// RateLimitFromConfig converts RPM to rate.Limit.
func RateLimitFromConfig(requestsPerMinute int) rate.Limit {
	if requestsPerMinute <= 0 {
		requestsPerMinute = 60
	}
	return rate.Every(time.Minute / time.Duration(requestsPerMinute))
}
