package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

// HealthRepository defines the interface for health checks
type HealthRepository interface {
	Ping(ctx context.Context) error
}

// HealthHandler handles health check requests
type HealthHandler struct {
	repo HealthRepository
	log  zerolog.Logger
}

// NewHealthHandler creates a new health handler
func NewHealthHandler(repo HealthRepository, log zerolog.Logger) *HealthHandler {
	return &HealthHandler{
		repo: repo,
		log:  log,
	}
}

// Health handles GET /health
func (h *HealthHandler) Health(c *gin.Context) {
	response := gin.H{
		"status":    "ok",
		"timestamp": time.Now().Unix(),
	}

	// Check ClickHouse but don't fail the health check if it's down
	if h.repo != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := h.repo.Ping(ctx); err != nil {
			h.log.Warn().Err(err).Msg("ClickHouse health check failed")
			response["clickhouse"] = gin.H{
				"status": "unhealthy",
				"error":  err.Error(),
			}
		} else {
			response["clickhouse"] = gin.H{
				"status": "healthy",
			}
		}
	} else {
		response["clickhouse"] = gin.H{
			"status": "not_configured",
		}
	}

	// Always return 200 for Docker/Kubernetes health checks
	// /health/ready checks storage readiness separately.
	c.JSON(http.StatusOK, response)
}

// Ready fails when storage is unavailable. Kafka delivery failures are retried
// by the consumer and reported through logs and metrics.
func (h *HealthHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if h.repo == nil {
		c.JSON(503, gin.H{"status": "not_ready"})
		return
	}
	if err := h.repo.Ping(ctx); err != nil {
		c.JSON(503, gin.H{"status": "not_ready"})
		return
	}
	c.JSON(200, gin.H{"status": "ready"})
}
