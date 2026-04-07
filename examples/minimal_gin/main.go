package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Minimal working example
func main() {
	// Use gin.Default() for built-in logging and recovery
	router := gin.Default()

	// Health endpoint - MUST come before any middleware that might abort
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"timestamp": time.Now().Unix(),
		})
	})

	// Metrics endpoint
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Protected routes with middleware
	api := router.Group("/api")
	{
		// Add middleware only to protected routes
		// api.Use(authMiddleware)
		api.GET("/example", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"message": "example"})
		})
	}

	// Start server
	if err := router.Run(":8080"); err != nil {
		panic(err)
	}
}
