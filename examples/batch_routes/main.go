// Example: How to register batch handler routes in your main router
// Add this to your router setup (e.g., cmd/api/main.go or internal/router/router.go)

//go:build ignore

package main

import (
	"github.com/gin-gonic/gin"
	"devops-fall2025-23b031152/internal/handler"
	"devops-fall2025-23b031152/internal/middleware"
)

func setupBatchRoutes(router *gin.Engine, batchHandler *handler.BatchHandler, authMiddleware, requireAdmin gin.HandlerFunc) {
	// Batch processing endpoints - all require admin role
	batchGroup := router.Group("/api/batch")
	batchGroup.Use(authMiddleware)
	batchGroup.Use(requireAdmin)
	{
		batchGroup.POST("/export", batchHandler.ExportBatchEvents)
		batchGroup.POST("/aggregate", batchHandler.AggregateBatchData)
		batchGroup.POST("/cleanup", batchHandler.CleanupTempFiles)
	}

	// Cleanup endpoints - all require admin role
	cleanupGroup := router.Group("/api/cleanup")
	cleanupGroup.Use(authMiddleware)
	cleanupGroup.Use(requireAdmin)
	{
		cleanupGroup.POST("/events", batchHandler.CleanupOldEvents)
		cleanupGroup.POST("/minio", batchHandler.CleanupMinIOFiles)
	}
}

// Full integration example:
func main() {
	router := gin.Default()
	
	// Initialize dependencies (exporter, repo, minioClient, auditRepo, logger)
	// ...
	
	// Create batch handler
	batchHandler := handler.NewBatchHandler(
		exporter,
		clickhouseRepo,
		minioClient,
		auditRepo,
		logger,
	)
	
	// Setup middlewares
	authMiddleware := middleware.AuthMiddleware(jwtManager, logger)
	requireAdmin := middleware.RequireAdmin(logger)
	
	// Register routes
	setupBatchRoutes(router, batchHandler, authMiddleware, requireAdmin)
	
	// Start server
	router.Run(":8080")
}
